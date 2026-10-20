#include "mcp/test_impact_result_internal.h"
#include <string.h>

tir_json *tir_node(tir_context *c, tir_kind kind) {
    tir_json *node = tir_alloc(c, 1, sizeof(*node));
    if (node)
        node->kind = kind;
    return node;
}
tir_json *tir_null(tir_context *c) {
    return tir_node(c, TIR_NULL);
}
tir_json *tir_bool(tir_context *c, bool value) {
    tir_json *node = tir_node(c, TIR_BOOL);
    if (node)
        node->as.boolean = value;
    return node;
}
tir_json *tir_uint(tir_context *c, uint64_t value) {
    tir_json *node = tir_node(c, TIR_UINT);
    if (node)
        node->as.number = value;
    return node;
}
tir_json *tir_int(tir_context *c, int64_t value) {
    tir_json *node = tir_node(c, TIR_INT);
    if (node)
        node->as.integer = value;
    return node;
}
tir_json *tir_string(tir_context *c, tir_bytes value) {
    tir_json *node = tir_node(c, TIR_STRING);
    if (node)
        node->as.string = value;
    return node;
}
tir_json *tir_text(tir_context *c, const char *value) {
    return tir_string(c, tir_literal(value));
}

bool tir_add(tir_context *c, tir_json *container, const char *key, tir_json *value) {
    if (!container || !value)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_step(c, 0, 1))
        return false;
    tir_link *entry = tir_alloc(c, 1, sizeof(*entry));
    if (!entry)
        return false;
    entry->key = key;
    entry->value = value;
    if (container->kind == TIR_ARRAY && !key) {
        if (container->as.children.last)
            container->as.children.last->next = entry;
        else
            container->as.children.first = entry;
        container->as.children.last = entry;
        return true;
    }
    if (container->kind != TIR_OBJECT || !key)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    tir_link **at = &container->as.children.first;
    while (*at) {
        int order = tir_compare(c, tir_literal((*at)->key), tir_literal(key));
        if (!tir_step(c, 0, 1))
            return false;
        if (!order)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        if (order > 0)
            break;
        at = &(*at)->next;
    }
    entry->next = *at;
    *at = entry;
    return true;
}

tir_json *tir_hex(tir_context *c, const unsigned char *data, size_t count) {
    static const char digits[] = "0123456789abcdef";
    if (count > (SIZE_MAX - 1) / 2) {
        tir_fail(c, CBM_TEST_RESULT_LIMIT);
        return NULL;
    }
    unsigned char *out = tir_alloc(c, count * 2 + 1, 1);
    if (!out)
        return NULL;
    for (size_t i = 0; i < count; i++) {
        if (!tir_step(c, 1, 0))
            return NULL;
        out[i * 2] = (unsigned char)digits[data[i] >> 4];
        out[i * 2 + 1] = (unsigned char)digits[data[i] & 15];
    }
    return tir_string(c, (tir_bytes){out, count * 2});
}

bool tir_path(tir_context *c, tir_json *object, tir_bytes file) {
    bool utf8 = tir_utf8(c, file);
    if (c->status != CBM_TEST_RESULT_OK)
        return false;
    tir_json *text = file.length && utf8 ? tir_string(c, file) : tir_null(c);
    tir_json *hex = file.length && !utf8 ? tir_hex(c, file.data, file.length) : tir_null(c);
    return tir_add(c, object, "file", text) && tir_add(c, object, "file_hex", hex);
}

tir_json *tir_digest(tir_context *c, cbm_test_result_digest_t digest) {
    return digest.present ? tir_hex(c, digest.bytes, 32) : tir_null(c);
}
tir_json *tir_oid(tir_context *c, cbm_test_result_oid_t oid) {
    if (oid.format == CBM_TEST_RESULT_OBJECT_UNKNOWN)
        return tir_null(c);
    return tir_hex(c, oid.bytes, oid.format == CBM_TEST_RESULT_OBJECT_SHA1 ? 20 : 32);
}

typedef struct {
    tir_context *context;
    char *bytes;
    size_t used, limit;
} tir_writer;

static bool tir_write(tir_writer *w, const void *bytes, size_t count) {
    if (count > w->limit - w->used)
        return tir_fail(w->context, CBM_TEST_RESULT_LIMIT);
    const unsigned char *source = bytes;
    for (size_t pos = 0; pos < count;) {
        size_t step = count - pos < 4096 ? count - pos : 4096;
        if (!tir_step(w->context, step, 0))
            return false;
        if (w->bytes)
            memcpy(w->bytes + w->used + pos, source + pos, step);
        pos += step;
    }
    w->used += count;
    return true;
}

static bool tir_write_literal(tir_writer *w, const char *s) {
    tir_bytes b = tir_literal(s);
    return tir_write(w, b.data, b.length);
}

static bool tir_write_string(tir_writer *w, tir_bytes b) {
    static const char hex[] = "0123456789abcdef";
    if (!tir_write(w, "\"", 1))
        return false;
    for (size_t i = 0; i < b.length; i++) {
        unsigned char ch = b.data[i];
        if (ch < 32) {
            char escaped[6] = {'\\', 'u', '0', '0', hex[ch >> 4], hex[ch & 15]};
            if (!tir_write(w, escaped, sizeof(escaped)))
                return false;
        } else if (ch == '\\' || ch == '"') {
            char escaped[2] = {'\\', (char)ch};
            if (!tir_write(w, escaped, sizeof(escaped)))
                return false;
        } else if (!tir_write(w, &ch, 1))
            return false;
    }
    return tir_write(w, "\"", 1);
}

static bool tir_write_number(tir_writer *w, uint64_t number, bool negative) {
    char buffer[21];
    size_t count = 0;
    do {
        buffer[count++] = (char)('0' + number % 10);
        number /= 10;
    } while (number);
    if (negative && !tir_write(w, "-", 1))
        return false;
    while (count)
        if (!tir_write(w, &buffer[--count], 1))
            return false;
    return true;
}

static bool tir_write_node(tir_writer *w, const tir_json *node);
static bool tir_write_children(tir_writer *w, const tir_json *node) {
    bool object = node->kind == TIR_OBJECT;
    if (!tir_write(w, object ? "{" : "[", 1))
        return false;
    bool first = true;
    for (const tir_link *entry = node->as.children.first; entry; entry = entry->next) {
        if (!tir_step(w->context, 0, 1))
            return false;
        if (!first && !tir_write(w, ",", 1))
            return false;
        first = false;
        if (object && (!tir_write_string(w, tir_literal(entry->key)) || !tir_write(w, ":", 1)))
            return false;
        if (!tir_write_node(w, entry->value))
            return false;
    }
    return tir_write(w, object ? "}" : "]", 1);
}

static bool tir_write_value(tir_writer *w, const tir_json *node) {
    if (!node || !tir_step(w->context, 0, 1))
        return false;
    switch (node->kind) {
    case TIR_NULL:
        return tir_write_literal(w, "null");
    case TIR_BOOL:
        return tir_write_literal(w, node->as.boolean ? "true" : "false");
    case TIR_UINT:
        return tir_write_number(w, node->as.number, false);
    case TIR_INT: {
        int64_t n = node->as.integer;
        uint64_t magnitude = n < 0 ? (uint64_t)(-(n + 1)) + 1 : (uint64_t)n;
        return tir_write_number(w, magnitude, n < 0);
    }
    case TIR_STRING:
        return tir_write_string(w, node->as.string);
    case TIR_ARRAY:
    case TIR_OBJECT:
        return tir_write_children(w, node);
    }
    return tir_fail(w->context, CBM_TEST_RESULT_INVALID);
}

static bool tir_write_node(tir_writer *w, const tir_json *node) {
    size_t start = w->used;
    if (!tir_write_value(w, node))
        return false;
    if (!w->bytes)
        return true;
    tir_context *c = w->context;
    if (node == c->lane_json) {
        c->lane_offset = start;
        c->lane_length = w->used - start;
    }
    if (node == c->global_json) {
        c->global_offset = start;
        c->global_length = w->used - start;
    }
    if (node == c->digest_json)
        c->digest_offset = start + 1;
    return true;
}

bool tir_render(tir_context *c, tir_json *node, char **bytes, size_t *length, bool output) {
    uint64_t bound = UINT64_MAX / 8;
    size_t limit = bound < SIZE_MAX - 1 ? (size_t)bound : SIZE_MAX - 1;
    if (output && c->limits->max_output_bytes < limit)
        limit = c->limits->max_output_bytes;
    tir_writer measure = {c, NULL, 0, limit};
    if (!tir_write_node(&measure, node))
        return false;
    char *data = tir_alloc(c, measure.used + 1, 1);
    if (!data)
        return false;
    tir_writer writer = {c, data, 0, measure.used};
    if (!tir_write_node(&writer, node))
        return false;
    *bytes = data;
    *length = writer.used;
    return true;
}

static bool tir_hash_part(tir_context *c, cbm_sha256_ctx *hash, tir_bytes bytes) {
    size_t length = bytes.length;
    for (size_t pos = 0; pos < length;) {
        size_t step = length - pos < 4096 ? length - pos : 4096;
        if (!tir_step(c, step, 0))
            return false;
        cbm_sha256_update(hash, bytes.data + pos, step);
        pos += step;
    }
    return true;
}

bool tir_decision_hash(tir_context *c, const char *data, size_t length, char out[65]) {
    static const char hex[] = "0123456789abcdef";
    if (c->lane_offset > length || c->lane_length > length - c->lane_offset ||
        c->global_offset > length || c->global_length > length - c->global_offset ||
        c->digest_offset > length || 64 > length - c->digest_offset)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    tir_bytes pieces[5] = {tir_literal("{\"lanes\":"),
                           {(const unsigned char *)data + c->lane_offset, c->lane_length},
                           tir_literal(",\"run_all_reasons\":"),
                           {(const unsigned char *)data + c->global_offset, c->global_length},
                           tir_literal("}")};
    uint64_t total = 0;
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    for (size_t i = 0; i < 5; i++) {
        if (!tir_charge(c, &total, pieces[i].length, UINT64_MAX / 8) ||
            !tir_hash_part(c, &hash, pieces[i]))
            return false;
    }
    unsigned char digest[32];
    cbm_sha256_final(&hash, digest);
    for (size_t i = 0; i < 32; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 15];
    }
    out[64] = 0;
    return true;
}
