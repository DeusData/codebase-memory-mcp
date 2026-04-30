/*
 * extract_channels.c — Pub/sub channel participation extractor.
 *
 * Detects event-driven communication patterns across multiple languages:
 *   JS/TS/TSX: Socket.IO, EventEmitter, raw WebSocket, Kafka, RabbitMQ
 *   Python:    python-socketio, Django Channels, FastAPI WebSocket, kafka-python
 *   Go:        gorilla/nhooyr websocket (WriteMessage/ReadMessage)
 *   Java:      JSR 356 WebSocket, Spring STOMP/WebSocket
 *   C#:        SignalR (Hub.SendAsync / Hub.On)
 *   Ruby:      ActionCable (broadcast / stream_from)
 *   Elixir:    Phoenix.PubSub, Phoenix.Channel
 *   Rust:      tokio-tungstenite (sink.send / stream.next)
 *
 * Transport is stored on the record ("socketio", "websocket", "kafka", etc.)
 * so later detectors can share the same schema without changing edge types.
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
    CHAN_DIR_UNKNOWN = -1, /* unrecognized method → no channel */
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
    if (strcmp(kind, "string") != 0 && strcmp(kind, "string_literal") != 0 &&
        strcmp(kind, "interpreted_string_literal") != 0 &&
        strcmp(kind, "raw_string_literal") != 0 && strcmp(kind, "string_content") != 0) {
        return NULL;
    }
    char *text = cbm_node_text(ctx->arena, arg, ctx->source);
    return unquote_string(ctx->arena, text);
}

/* Extract string literal from first named child (for nodes wrapping string content). */
static const char *literal_from_first_child(CBMExtractCtx *ctx, TSNode node) {
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *val = literal_from_arg(ctx, child);
        if (val) {
            return val;
        }
    }
    return NULL;
}

/* ── Constant resolution table ──────────────────────────────────── */

/* Walk the whole tree once and collect constant string bindings so later
 * passes can resolve bare-identifier channel arguments. */
static void scan_string_consts_js(CBMExtractCtx *ctx, chan_const_table_t *tbl) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0 && tbl->count < CHAN_CONST_CAP) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);

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

/* Python constant resolution: NAME = "value" (assignment node). */
static void scan_string_consts_python(CBMExtractCtx *ctx, chan_const_table_t *tbl) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0 && tbl->count < CHAN_CONST_CAP) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);

        if (strcmp(kind, "assignment") == 0) {
            TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
            TSNode right = ts_node_child_by_field_name(node, TS_FIELD("right"));
            if (!ts_node_is_null(left) && !ts_node_is_null(right) &&
                strcmp(ts_node_type(left), "identifier") == 0 &&
                strcmp(ts_node_type(right), "string") == 0) {
                char *name = cbm_node_text(ctx->arena, left, ctx->source);
                const char *val = literal_from_arg(ctx, right);
                if (!val) {
                    val = literal_from_first_child(ctx, right);
                }
                if (name && val) {
                    tbl->items[tbl->count].name = name;
                    tbl->items[tbl->count].value = val;
                    tbl->count++;
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

static const char *enclosing_function_qn(CBMExtractCtx *ctx, TSNode node) {
    TSNode parent = ts_node_parent(node);
    while (!ts_node_is_null(parent)) {
        const char *pk = ts_node_type(parent);
        if (strcmp(pk, "function_declaration") == 0 || strcmp(pk, "method_definition") == 0 ||
            strcmp(pk, "arrow_function") == 0 || strcmp(pk, "function_expression") == 0 ||
            strcmp(pk, "function") == 0 || strcmp(pk, "method_signature") == 0 ||
            strcmp(pk, "function_definition") == 0 || strcmp(pk, "method_declaration") == 0 ||
            strcmp(pk, "function_item") == 0 || strcmp(pk, "def") == 0) {
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

/* ── Channel name extraction from arguments ──────────────────────── */

/* Try to extract a channel name from the first argument of a call.
 * Tries literal first, then identifier resolution via constant table. */
static const char *extract_channel_name(CBMExtractCtx *ctx, TSNode args,
                                        const chan_const_table_t *consts) {
    uint32_t arg_count = ts_node_named_child_count(args);
    if (arg_count == 0) {
        return NULL;
    }
    TSNode first = ts_node_named_child(args, 0);

    const char *channel_name = literal_from_arg(ctx, first);
    if (!channel_name) {
        channel_name = literal_from_first_child(ctx, first);
    }
    if (!channel_name && consts) {
        const char *kind = ts_node_type(first);
        if (strcmp(kind, "identifier") == 0) {
            char *ident = cbm_node_text(ctx->arena, first, ctx->source);
            channel_name = resolve_identifier(consts, ident);
        }
    }
    return channel_name;
}

/* ── Emit helper ─────────────────────────────────────────────────── */

static void push_channel(CBMExtractCtx *ctx, const char *channel_name, const char *transport,
                         CBMChannelDirection direction, TSNode call) {
    CBMChannel ch = {
        .channel_name = channel_name,
        .transport = transport,
        .enclosing_func_qn = enclosing_function_qn(ctx, call),
        .direction = direction,
    };
    cbm_channels_push(&ctx->result->channels, ctx->arena, ch);
}

/* ══════════════════════════════════════════════════════════════════
 *  JS/TS/TSX — Socket.IO, EventEmitter, WebSocket, Kafka, RabbitMQ
 * ══════════════════════════════════════════════════════════════════ */

static bool js_is_emit_method(const char *name) {
    return name && strcmp(name, "emit") == 0;
}

static bool js_is_listen_method(const char *name) {
    return name && (strcmp(name, "on") == 0 || strcmp(name, "addListener") == 0 ||
                    strcmp(name, "once") == 0);
}

/* Classify receiver for Socket.IO / EventEmitter / WebSocket. */
static const char *js_classify_receiver(CBMExtractCtx *ctx, TSNode object_node) {
    char *text = cbm_node_text(ctx->arena, object_node, ctx->source);
    if (!text) {
        return NULL;
    }
    const char *tail = text;
    const char *dot = strrchr(tail, '.');
    if (dot) {
        tail = dot + SKIP_ONE;
    }
    /* Socket.IO */
    if (strcmp(tail, "socket") == 0 || strcmp(tail, "io") == 0 || strcmp(tail, "ws") == 0 ||
        strcmp(tail, "client") == 0 || strcmp(tail, "server") == 0) {
        return "socketio";
    }
    /* Node.js EventEmitter */
    if (strcmp(tail, "emitter") == 0 || strcmp(tail, "eventEmitter") == 0 ||
        strcmp(tail, "events") == 0 || strcmp(tail, "bus") == 0 || strcmp(tail, "eventBus") == 0 ||
        strcmp(tail, "pubsub") == 0) {
        return "event_emitter";
    }
    /* Kafka */
    if (strcmp(tail, "producer") == 0) {
        return "kafka";
    }
    if (strcmp(tail, "consumer") == 0) {
        return "kafka";
    }
    /* RabbitMQ / AMQP */
    if (strcmp(tail, "channel") == 0 && !strcmp(text, "channel")) {
        return "rabbitmq";
    }
    return NULL;
}

/* Detect Kafka/RabbitMQ specific send/subscribe patterns. */
static bool js_is_kafka_send(const char *method) {
    return method && (strcmp(method, "send") == 0 || strcmp(method, "sendBatch") == 0);
}

static bool js_is_kafka_listen(const char *method) {
    return method && (strcmp(method, "subscribe") == 0 || strcmp(method, "run") == 0);
}

static bool js_is_amqp_send(const char *method) {
    return method && (strcmp(method, "publish") == 0 || strcmp(method, "sendToQueue") == 0);
}

static bool js_is_amqp_listen(const char *method) {
    return method && (strcmp(method, "consume") == 0 || strcmp(method, "assertQueue") == 0);
}

static void js_process_call(CBMExtractCtx *ctx, TSNode call, const chan_const_table_t *consts) {
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
    const char *transport = js_classify_receiver(ctx, object);
    if (!transport) {
        return;
    }

    CBMChannelDirection direction;
    if (strcmp(transport, "kafka") == 0) {
        if (js_is_kafka_send(method)) {
            direction = CBM_CHANNEL_EMIT;
        } else if (js_is_kafka_listen(method)) {
            direction = CBM_CHANNEL_LISTEN;
        } else {
            return;
        }
    } else if (strcmp(transport, "rabbitmq") == 0) {
        if (js_is_amqp_send(method)) {
            direction = CBM_CHANNEL_EMIT;
        } else if (js_is_amqp_listen(method)) {
            direction = CBM_CHANNEL_LISTEN;
        } else {
            return;
        }
    } else {
        /* socketio / event_emitter */
        if (js_is_emit_method(method)) {
            direction = CBM_CHANNEL_EMIT;
        } else if (js_is_listen_method(method)) {
            direction = CBM_CHANNEL_LISTEN;
        } else {
            return;
        }
    }

    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return;
    }
    const char *channel_name = extract_channel_name(ctx, args, consts);
    if (!channel_name) {
        return;
    }
    push_channel(ctx, channel_name, transport, direction, call);
}

static void extract_channels_js(CBMExtractCtx *ctx) {
    chan_const_table_t consts = {0};
    scan_string_consts_js(ctx, &consts);

    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "call_expression") == 0) {
            js_process_call(ctx, node, &consts);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Python — python-socketio, Django Channels, FastAPI WebSocket, Kafka
 * ══════════════════════════════════════════════════════════════════ */

static const char *py_classify_receiver(CBMExtractCtx *ctx, TSNode object_node) {
    char *text = cbm_node_text(ctx->arena, object_node, ctx->source);
    if (!text) {
        return NULL;
    }
    const char *tail = text;
    const char *dot = strrchr(tail, '.');
    if (dot) {
        tail = dot + SKIP_ONE;
    }
    /* python-socketio */
    if (strcmp(tail, "sio") == 0 || strcmp(tail, "socketio") == 0 || strcmp(tail, "socket") == 0) {
        return "socketio";
    }
    /* Django Channels */
    if (strcmp(tail, "channel_layer") == 0) {
        return "django_channels";
    }
    /* FastAPI/Starlette WebSocket */
    if (strcmp(tail, "websocket") == 0 || strcmp(tail, "ws") == 0) {
        return "websocket";
    }
    /* kafka-python */
    if (strcmp(tail, "producer") == 0) {
        return "kafka";
    }
    if (strcmp(tail, "consumer") == 0) {
        return "kafka";
    }
    return NULL;
}

/* Table-driven Python method→direction classification.
 * NULL transport matches any transport not matched by earlier rows. */
static const struct {
    const char *transport; /* NULL = wildcard (socketio fallback) */
    const char *method;
    int direction;
} py_method_table[] = {
    {"kafka", "send", CBM_CHANNEL_EMIT},
    {"kafka", "produce", CBM_CHANNEL_EMIT},
    {"kafka", "subscribe", CBM_CHANNEL_LISTEN},
    {"kafka", "poll", CBM_CHANNEL_LISTEN},
    {"django_channels", "send", CBM_CHANNEL_EMIT},
    {"django_channels", "group_send", CBM_CHANNEL_EMIT},
    {"django_channels", "receive", CBM_CHANNEL_LISTEN},
    {"django_channels", "group_add", CBM_CHANNEL_LISTEN},
    {"websocket", "send", CBM_CHANNEL_EMIT},
    {"websocket", "send_text", CBM_CHANNEL_EMIT},
    {"websocket", "send_json", CBM_CHANNEL_EMIT},
    {"websocket", "send_bytes", CBM_CHANNEL_EMIT},
    {"websocket", "receive", CBM_CHANNEL_LISTEN},
    {"websocket", "receive_text", CBM_CHANNEL_LISTEN},
    {"websocket", "receive_json", CBM_CHANNEL_LISTEN},
    {"websocket", "receive_bytes", CBM_CHANNEL_LISTEN},
    {NULL, "emit", CBM_CHANNEL_EMIT},
    {NULL, "send", CBM_CHANNEL_EMIT},
    {NULL, "on", CBM_CHANNEL_LISTEN},
    {NULL, NULL, CHAN_DIR_UNKNOWN},
};

static int py_classify_direction(const char *transport, const char *method) {
    for (int i = 0; py_method_table[i].method != NULL; i++) {
        const char *t = py_method_table[i].transport;
        if (t && strcmp(t, transport) != 0) {
            continue;
        }
        if (strcmp(py_method_table[i].method, method) == 0) {
            return py_method_table[i].direction;
        }
    }
    return CHAN_DIR_UNKNOWN;
}

static void py_process_call(CBMExtractCtx *ctx, TSNode call, const chan_const_table_t *consts) {
    /* Python call: attribute { object, attribute }, argument_list */
    TSNode func = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(func)) {
        return;
    }
    const char *fk = ts_node_type(func);
    if (strcmp(fk, "attribute") != 0) {
        return;
    }
    TSNode object = ts_node_child_by_field_name(func, TS_FIELD("object"));
    TSNode attr = ts_node_child_by_field_name(func, TS_FIELD("attribute"));
    if (ts_node_is_null(object) || ts_node_is_null(attr)) {
        return;
    }

    char *method = cbm_node_text(ctx->arena, attr, ctx->source);
    const char *transport = py_classify_receiver(ctx, object);
    if (!transport || !method) {
        return;
    }

    int dir = py_classify_direction(transport, method);
    if (dir == CHAN_DIR_UNKNOWN) {
        return;
    }
    CBMChannelDirection direction = (CBMChannelDirection)dir;

    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return;
    }
    const char *channel_name = extract_channel_name(ctx, args, consts);
    if (!channel_name) {
        return;
    }
    push_channel(ctx, channel_name, transport, direction, call);
}

/* Detect Python decorator-based listeners: @sio.on("event") / @sio.event */
static void py_process_decorator(CBMExtractCtx *ctx, TSNode decorator,
                                 const chan_const_table_t *consts) {
    /* decorator: @expression or @call(args) */
    uint32_t nc = ts_node_named_child_count(decorator);
    if (nc == 0) {
        return;
    }
    TSNode expr = ts_node_named_child(decorator, 0);
    const char *ek = ts_node_type(expr);

    /* @sio.on("event") → call node wrapping attribute access */
    if (strcmp(ek, "call") == 0) {
        TSNode func = ts_node_child_by_field_name(expr, TS_FIELD("function"));
        if (ts_node_is_null(func) || strcmp(ts_node_type(func), "attribute") != 0) {
            return;
        }
        TSNode object = ts_node_child_by_field_name(func, TS_FIELD("object"));
        TSNode attr = ts_node_child_by_field_name(func, TS_FIELD("attribute"));
        if (ts_node_is_null(object) || ts_node_is_null(attr)) {
            return;
        }
        char *method = cbm_node_text(ctx->arena, attr, ctx->source);
        if (!method || strcmp(method, "on") != 0) {
            return;
        }
        const char *transport = py_classify_receiver(ctx, object);
        if (!transport) {
            return;
        }
        TSNode args = ts_node_child_by_field_name(expr, TS_FIELD("arguments"));
        if (ts_node_is_null(args)) {
            return;
        }
        const char *channel_name = extract_channel_name(ctx, args, consts);
        if (!channel_name) {
            return;
        }
        push_channel(ctx, channel_name, transport, CBM_CHANNEL_LISTEN, decorator);
    }
}

static void extract_channels_python(CBMExtractCtx *ctx) {
    chan_const_table_t consts = {0};
    scan_string_consts_python(ctx, &consts);

    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "call") == 0) {
            py_process_call(ctx, node, &consts);
        } else if (strcmp(kind, "decorator") == 0) {
            py_process_decorator(ctx, node, &consts);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Go — gorilla/nhooyr websocket (WriteMessage/ReadMessage)
 * ══════════════════════════════════════════════════════════════════ */

static void go_process_call(CBMExtractCtx *ctx, TSNode call) {
    TSNode func = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(func)) {
        return;
    }
    const char *fk = ts_node_type(func);
    if (strcmp(fk, "selector_expression") != 0) {
        return;
    }
    TSNode field = ts_node_child_by_field_name(func, TS_FIELD("field"));
    TSNode operand = ts_node_child_by_field_name(func, TS_FIELD("operand"));
    if (ts_node_is_null(field) || ts_node_is_null(operand)) {
        return;
    }

    char *method = cbm_node_text(ctx->arena, field, ctx->source);
    if (!method) {
        return;
    }

    /* gorilla/nhooyr websocket patterns */
    CBMChannelDirection direction;
    if (strcmp(method, "WriteMessage") == 0 || strcmp(method, "WriteJSON") == 0 ||
        strcmp(method, "Write") == 0) {
        direction = CBM_CHANNEL_EMIT;
    } else if (strcmp(method, "ReadMessage") == 0 || strcmp(method, "ReadJSON") == 0 ||
               strcmp(method, "Read") == 0) {
        direction = CBM_CHANNEL_LISTEN;
    } else {
        return;
    }

    /* Verify receiver looks like a websocket connection */
    char *recv = cbm_node_text(ctx->arena, operand, ctx->source);
    if (!recv) {
        return;
    }
    const char *tail = recv;
    const char *dot = strrchr(tail, '.');
    if (dot) {
        tail = dot + SKIP_ONE;
    }
    if (strcmp(tail, "conn") != 0 && strcmp(tail, "wsConn") != 0 && strcmp(tail, "ws") != 0 &&
        strcmp(tail, "c") != 0 && strcmp(tail, "Conn") != 0 && strcmp(tail, "connection") != 0) {
        return;
    }

    /* Go WebSocket uses connection-level send/receive, not named channels.
     * Use the enclosing function name as a pseudo-channel for cross-repo matching. */
    const char *func_name = enclosing_function_qn(ctx, call);
    const char *channel_name = func_name ? func_name : "(websocket)";
    push_channel(ctx, channel_name, "websocket", direction, call);
}

static void extract_channels_go(CBMExtractCtx *ctx) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "call_expression") == 0) {
            go_process_call(ctx, node);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Java — JSR 356 WebSocket, Spring STOMP/WebSocket
 * ══════════════════════════════════════════════════════════════════ */

static void java_process_call(CBMExtractCtx *ctx, TSNode call) {
    TSNode func = ts_node_child_by_field_name(call, TS_FIELD("name"));
    TSNode object = ts_node_child_by_field_name(call, TS_FIELD("object"));
    if (ts_node_is_null(func)) {
        return;
    }

    char *method = cbm_node_text(ctx->arena, func, ctx->source);
    if (!method) {
        return;
    }

    /* Spring STOMP: template.convertAndSend("/topic/...", msg) */
    if (strcmp(method, "convertAndSend") == 0 || strcmp(method, "convertAndSendToUser") == 0) {
        TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
        if (ts_node_is_null(args)) {
            return;
        }
        const char *channel_name = extract_channel_name(ctx, args, NULL);
        if (channel_name) {
            push_channel(ctx, channel_name, "spring_websocket", CBM_CHANNEL_EMIT, call);
        }
        return;
    }

    /* JSR 356: session.getBasicRemote().sendText(msg) — detect sendText/sendObject */
    if (strcmp(method, "sendText") == 0 || strcmp(method, "sendObject") == 0 ||
        strcmp(method, "sendBinary") == 0) {
        const char *func_name = enclosing_function_qn(ctx, call);
        const char *channel_name = func_name ? func_name : "(websocket)";
        push_channel(ctx, channel_name, "websocket", CBM_CHANNEL_EMIT, call);
        return;
    }

    (void)object;
}

/* Detect Java annotation-based WebSocket listeners: @OnMessage, @MessageMapping */
static void java_process_annotation(CBMExtractCtx *ctx, TSNode annotation) {
    TSNode name_node = ts_node_child_by_field_name(annotation, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name) {
        return;
    }

    if (strcmp(name, "OnMessage") == 0 || strcmp(name, "OnOpen") == 0 ||
        strcmp(name, "OnClose") == 0) {
        const char *func_name = enclosing_function_qn(ctx, annotation);
        const char *channel_name = func_name ? func_name : "(websocket)";
        push_channel(ctx, channel_name, "websocket", CBM_CHANNEL_LISTEN, annotation);
    } else if (strcmp(name, "MessageMapping") == 0) {
        /* @MessageMapping("/path") — extract the path */
        TSNode args = ts_node_child_by_field_name(annotation, TS_FIELD("arguments"));
        if (!ts_node_is_null(args)) {
            const char *path = extract_channel_name(ctx, args, NULL);
            if (path) {
                push_channel(ctx, path, "spring_websocket", CBM_CHANNEL_LISTEN, annotation);
                return;
            }
        }
        push_channel(ctx, "(spring_ws)", "spring_websocket", CBM_CHANNEL_LISTEN, annotation);
    } else if (strcmp(name, "ServerEndpoint") == 0) {
        TSNode args = ts_node_child_by_field_name(annotation, TS_FIELD("arguments"));
        if (!ts_node_is_null(args)) {
            const char *path = extract_channel_name(ctx, args, NULL);
            if (path) {
                push_channel(ctx, path, "websocket", CBM_CHANNEL_LISTEN, annotation);
                return;
            }
        }
    }
}

static void extract_channels_java(CBMExtractCtx *ctx) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "method_invocation") == 0) {
            java_process_call(ctx, node);
        } else if (strcmp(kind, "marker_annotation") == 0 || strcmp(kind, "annotation") == 0) {
            java_process_annotation(ctx, node);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  C# — SignalR (Clients.All.SendAsync / Hub.On)
 * ══════════════════════════════════════════════════════════════════ */

static void csharp_process_call(CBMExtractCtx *ctx, TSNode call) {
    TSNode func = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(func)) {
        return;
    }
    const char *fk = ts_node_type(func);
    if (strcmp(fk, "member_access_expression") != 0) {
        return;
    }
    TSNode name_node = ts_node_child_by_field_name(func, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *method = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!method) {
        return;
    }

    /* SignalR: Clients.All.SendAsync("method", data) */
    if (strcmp(method, "SendAsync") == 0 || strcmp(method, "SendCoreAsync") == 0) {
        char *full = cbm_node_text(ctx->arena, func, ctx->source);
        if (full && (strstr(full, "Clients") || strstr(full, "clients"))) {
            TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
            if (!ts_node_is_null(args)) {
                const char *channel_name = extract_channel_name(ctx, args, NULL);
                if (channel_name) {
                    push_channel(ctx, channel_name, "signalr", CBM_CHANNEL_EMIT, call);
                }
            }
        }
        return;
    }

    /* SignalR: connection.On<T>("method", handler) */
    if (strcmp(method, "On") == 0) {
        TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
        if (!ts_node_is_null(args)) {
            const char *channel_name = extract_channel_name(ctx, args, NULL);
            if (channel_name) {
                push_channel(ctx, channel_name, "signalr", CBM_CHANNEL_LISTEN, call);
            }
        }
    }
}

static void extract_channels_csharp(CBMExtractCtx *ctx) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "invocation_expression") == 0) {
            csharp_process_call(ctx, node);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Ruby — ActionCable (broadcast / stream_from)
 * ══════════════════════════════════════════════════════════════════ */

static void ruby_process_call(CBMExtractCtx *ctx, TSNode call) {
    const char *kind = ts_node_type(call);
    TSNode method_node;

    if (strcmp(kind, "call") == 0) {
        method_node = ts_node_child_by_field_name(call, TS_FIELD("method"));
    } else {
        return;
    }

    if (ts_node_is_null(method_node)) {
        return;
    }
    char *method = cbm_node_text(ctx->arena, method_node, ctx->source);
    if (!method) {
        return;
    }

    /* ActionCable.server.broadcast("channel", data) */
    if (strcmp(method, "broadcast") == 0) {
        TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
        if (!ts_node_is_null(args)) {
            const char *channel_name = extract_channel_name(ctx, args, NULL);
            if (channel_name) {
                push_channel(ctx, channel_name, "actioncable", CBM_CHANNEL_EMIT, call);
            }
        }
        return;
    }

    /* stream_from "channel" — listener registration */
    if (strcmp(method, "stream_from") == 0 || strcmp(method, "stream_for") == 0) {
        TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
        if (!ts_node_is_null(args)) {
            const char *channel_name = extract_channel_name(ctx, args, NULL);
            if (channel_name) {
                push_channel(ctx, channel_name, "actioncable", CBM_CHANNEL_LISTEN, call);
            }
        }
    }
}

static void extract_channels_ruby(CBMExtractCtx *ctx) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "call") == 0) {
            ruby_process_call(ctx, node);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Elixir — Phoenix.PubSub, Phoenix.Channel
 * ══════════════════════════════════════════════════════════════════ */

/* Extract a string literal from the Nth named child of an args node. */
static const char *elixir_nth_arg_literal(CBMExtractCtx *ctx, TSNode args, uint32_t index) {
    uint32_t ac = ts_node_named_child_count(args);
    if (ac <= index) {
        return NULL;
    }
    TSNode arg = ts_node_named_child(args, index);
    const char *val = literal_from_arg(ctx, arg);
    if (!val) {
        val = literal_from_first_child(ctx, arg);
    }
    return val;
}

/* Try to emit a channel from the second argument of an Elixir call. */
static void elixir_emit_second_arg(CBMExtractCtx *ctx, TSNode call, TSNode args,
                                   const char *transport, CBMChannelDirection direction) {
    if (ts_node_is_null(args)) {
        return;
    }
    const char *val = elixir_nth_arg_literal(ctx, args, SKIP_ONE);
    if (val) {
        push_channel(ctx, val, transport, direction, call);
    }
}

static void elixir_process_call(CBMExtractCtx *ctx, TSNode call) {
    TSNode target = ts_node_child_by_field_name(call, TS_FIELD("target"));
    if (ts_node_is_null(target)) {
        return;
    }
    char *target_text = cbm_node_text(ctx->arena, target, ctx->source);
    if (!target_text) {
        return;
    }

    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));

    if (strstr(target_text, "PubSub.broadcast") || strstr(target_text, "PubSub.local_broadcast")) {
        elixir_emit_second_arg(ctx, call, args, "phoenix_pubsub", CBM_CHANNEL_EMIT);
        return;
    }
    if (strstr(target_text, "PubSub.subscribe")) {
        elixir_emit_second_arg(ctx, call, args, "phoenix_pubsub", CBM_CHANNEL_LISTEN);
        return;
    }
    if (strcmp(target_text, "push") == 0 || strcmp(target_text, "broadcast") == 0 ||
        strcmp(target_text, "broadcast!") == 0) {
        elixir_emit_second_arg(ctx, call, args, "phoenix_channel", CBM_CHANNEL_EMIT);
    }
}

/* Detect handle_in("event", payload, socket) — Phoenix Channel listener */
static void elixir_process_function_def(CBMExtractCtx *ctx, TSNode func_def) {
    TSNode name_node = ts_node_child_by_field_name(func_def, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || strcmp(name, "handle_in") != 0) {
        return;
    }
    /* First parameter is the event name pattern */
    TSNode params = ts_node_child_by_field_name(func_def, TS_FIELD("parameters"));
    if (ts_node_is_null(params)) {
        return;
    }
    uint32_t pc = ts_node_named_child_count(params);
    if (pc == 0) {
        return;
    }
    TSNode first_param = ts_node_named_child(params, 0);
    const char *val = literal_from_arg(ctx, first_param);
    if (!val) {
        val = literal_from_first_child(ctx, first_param);
    }
    if (val) {
        push_channel(ctx, val, "phoenix_channel", CBM_CHANNEL_LISTEN, func_def);
    }
}

static void extract_channels_elixir(CBMExtractCtx *ctx) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "call") == 0) {
            elixir_process_call(ctx, node);
        } else if (strcmp(kind, "def") == 0) {
            elixir_process_function_def(ctx, node);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Rust — tokio-tungstenite (sink.send / stream.next)
 * ══════════════════════════════════════════════════════════════════ */

static void rust_process_call(CBMExtractCtx *ctx, TSNode call) {
    TSNode func = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(func)) {
        return;
    }
    const char *fk = ts_node_type(func);
    if (strcmp(fk, "field_expression") != 0) {
        return;
    }
    TSNode field = ts_node_child_by_field_name(func, TS_FIELD("field"));
    TSNode value = ts_node_child_by_field_name(func, TS_FIELD("value"));
    if (ts_node_is_null(field) || ts_node_is_null(value)) {
        return;
    }

    char *method = cbm_node_text(ctx->arena, field, ctx->source);
    if (!method) {
        return;
    }

    CBMChannelDirection direction;
    if (strcmp(method, "send") == 0 || strcmp(method, "send_all") == 0 ||
        strcmp(method, "feed") == 0) {
        direction = CBM_CHANNEL_EMIT;
    } else if (strcmp(method, "next") == 0 || strcmp(method, "try_next") == 0) {
        direction = CBM_CHANNEL_LISTEN;
    } else {
        return;
    }

    /* Verify receiver looks like a websocket sink/stream */
    char *recv = cbm_node_text(ctx->arena, value, ctx->source);
    if (!recv) {
        return;
    }
    const char *tail = recv;
    const char *dot = strrchr(tail, '.');
    if (dot) {
        tail = dot + SKIP_ONE;
    }
    if (strcmp(tail, "sink") != 0 && strcmp(tail, "ws_sender") != 0 &&
        strcmp(tail, "writer") != 0 && strcmp(tail, "write") != 0 && strcmp(tail, "stream") != 0 &&
        strcmp(tail, "ws_receiver") != 0 && strcmp(tail, "reader") != 0 &&
        strcmp(tail, "read") != 0 && strcmp(tail, "ws_stream") != 0 && strcmp(tail, "ws") != 0) {
        return;
    }

    const char *func_name = enclosing_function_qn(ctx, call);
    const char *channel_name = func_name ? func_name : "(websocket)";
    push_channel(ctx, channel_name, "websocket", direction, call);
}

static void extract_channels_rust(CBMExtractCtx *ctx) {
    TSNode stack[CHAN_STACK_CAP];
    int top = 0;
    stack[top++] = ctx->root;

    while (top > 0) {
        TSNode node = stack[--top];
        if (strcmp(ts_node_type(node), "call_expression") == 0) {
            rust_process_call(ctx, node);
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < CHAN_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  Entry point — language dispatch
 * ══════════════════════════════════════════════════════════════════ */

void cbm_extract_channels(CBMExtractCtx *ctx) {
    switch (ctx->language) {
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        extract_channels_js(ctx);
        break;
    case CBM_LANG_PYTHON:
        extract_channels_python(ctx);
        break;
    case CBM_LANG_GO:
        extract_channels_go(ctx);
        break;
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
        extract_channels_java(ctx);
        break;
    case CBM_LANG_CSHARP:
        extract_channels_csharp(ctx);
        break;
    case CBM_LANG_RUBY:
        extract_channels_ruby(ctx);
        break;
    case CBM_LANG_ELIXIR:
        extract_channels_elixir(ctx);
        break;
    case CBM_LANG_RUST:
        extract_channels_rust(ctx);
        break;
    default:
        break; /* no channel detection for this language */
    }
}
