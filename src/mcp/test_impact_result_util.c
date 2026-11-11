#include "mcp/test_impact_result_internal.h"
#include <string.h>

bool tir_fail(tir_context *c, cbm_test_result_status_t status) {
    if (c->status == CBM_TEST_RESULT_OK)
        c->status = status;
    return false;
}

bool tir_poll(tir_context *c) {
    if (c->status != CBM_TEST_RESULT_OK)
        return false;
    if (c->cancelled && c->cancelled(c->cancel_context))
        return tir_fail(c, CBM_TEST_RESULT_CANCELLED);
    return true;
}

bool tir_step(tir_context *c, size_t bytes, size_t items) {
    if (c->status != CBM_TEST_RESULT_OK)
        return false;
    c->byte_steps += bytes;
    c->item_steps += items;
    if (c->byte_steps < 4096 && c->item_steps < 1024)
        return true;
    c->byte_steps = 0;
    c->item_steps = 0;
    return tir_poll(c);
}

bool tir_charge(tir_context *c, uint64_t *used, uint64_t amount, uint64_t limit) {
    if (*used > limit || amount > limit - *used)
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    *used += amount;
    return true;
}

bool tir_items(tir_context *c, uint64_t count) {
    return tir_charge(c, &c->items, count, c->limits->max_items);
}

bool tir_input_bytes(tir_context *c, size_t count) {
    return tir_charge(c, &c->input_bytes, count, c->limits->max_input_bytes);
}

bool tir_reserve(tir_context *c, size_t count) {
    if (count > c->limits->max_alloc_bytes - c->allocations)
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    c->allocations += count;
    return true;
}

bool tir_copy(tir_context *c, void *dest, const void *source, size_t count) {
    unsigned char *out = dest;
    const unsigned char *in = source;
    for (size_t pos = 0; pos < count;) {
        size_t step = count - pos < 4096 ? count - pos : 4096;
        if (!tir_step(c, step, 0))
            return false;
        memcpy(out + pos, in + pos, step);
        pos += step;
    }
    return true;
}

void *tir_alloc(tir_context *c, size_t count, size_t size) {
    if (!count || !size || c->status != CBM_TEST_RESULT_OK)
        return NULL;
    if (count > (SIZE_MAX - 7 - sizeof(tir_allocation)) / size) {
        tir_fail(c, CBM_TEST_RESULT_LIMIT);
        return NULL;
    }
    size_t payload = count * size;
    size_t bytes = payload + sizeof(tir_allocation);
    size_t aligned = (bytes + 7) & ~(size_t)7;
    if (aligned > SIZE_MAX - c->arena.used || aligned > SIZE_MAX - c->arena.total_alloc ||
        !tir_reserve(c, bytes) || !tir_poll(c)) {
        tir_fail(c, CBM_TEST_RESULT_LIMIT);
        return NULL;
    }
    void *out = cbm_arena_alloc(&c->arena, bytes);
    if (!out) {
        tir_fail(c, CBM_TEST_RESULT_OOM);
        return NULL;
    }
    for (size_t pos = 0; pos < bytes;) {
        size_t step = bytes - pos < 4096 ? bytes - pos : 4096;
        if (!tir_step(c, step, 0))
            return NULL;
        memset((unsigned char *)out + pos, 0, step);
        pos += step;
    }
    tir_allocation *entry = out;
    entry->next = c->owned;
    entry->size = payload;
    entry->data = entry + 1;
    c->owned = entry;
    return entry->data;
}

bool tir_forget_borrowed(tir_context *c, const cbm_test_result_t *result) {
    tir_allocation *entry = c->owned;
    while (entry) {
        tir_allocation *next = entry->next;
        if (!tir_step(c, 0, 1))
            return false;
        if (entry->data != result && entry->data != result->json) {
            for (size_t pos = 0; pos < entry->size;) {
                size_t step = entry->size - pos < 4096 ? entry->size - pos : 4096;
                if (!tir_step(c, step, 0))
                    return false;
                memset((unsigned char *)entry->data + pos, 0, step);
                pos += step;
            }
        }
        memset(entry, 0, sizeof(*entry));
        entry = next;
    }
    c->owned = NULL;
    return true;
}

tir_bytes tir_literal(const char *s) {
    /* Only bounded internal literals and the fixed 64-hex digest use this. */
    tir_bytes b = {(const unsigned char *)s, 0};
    while (s[b.length])
        b.length++;
    return b;
}

bool tir_cstring(tir_context *c, const char *s, tir_bytes *out) {
    if (!s)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    size_t length = 0;
    for (;;) {
        if (!tir_input_bytes(c, 1) || !tir_step(c, 1, 0))
            return false;
        if (!s[length])
            break;
        if (length == SIZE_MAX - 1)
            return tir_fail(c, CBM_TEST_RESULT_LIMIT);
        length++;
    }
    *out = (tir_bytes){(const unsigned char *)s, length};
    return true;
}

static bool tir_utf8_tail(tir_context *c, tir_bytes b, size_t *at, unsigned count, uint32_t value,
                          uint32_t minimum) {
    if (count > b.length - *at)
        return false;
    for (unsigned i = 0; i < count; i++) {
        if (!tir_step(c, 1, 0))
            return false;
        unsigned ch = b.data[(*at)++];
        if ((ch & 0xc0) != 0x80)
            return false;
        value = (value << 6) | (ch & 0x3f);
    }
    return value >= minimum && value <= 0x10ffff && !(value >= 0xd800 && value <= 0xdfff);
}

bool tir_utf8(tir_context *c, tir_bytes b) {
    for (size_t i = 0; i < b.length;) {
        if (!tir_step(c, 1, 0))
            return false;
        unsigned ch = b.data[i++];
        if (ch < 0x80)
            continue;
        unsigned count;
        uint32_t value, minimum;
        if (ch >= 0xc2 && ch <= 0xdf) {
            count = 1;
            value = ch & 0x1f;
            minimum = 0x80;
        } else if (ch >= 0xe0 && ch <= 0xef) {
            count = 2;
            value = ch & 0x0f;
            minimum = 0x800;
        } else if (ch >= 0xf0 && ch <= 0xf4) {
            count = 3;
            value = ch & 7;
            minimum = 0x10000;
        } else {
            return false;
        }
        if (!tir_utf8_tail(c, b, &i, count, value, minimum))
            return false;
    }
    return true;
}

bool tir_span(tir_context *c, tir_bytes b, bool text) {
    if (b.length && !b.data)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_input_bytes(c, b.length))
        return false;
    if (text && !tir_utf8(c, b))
        return tir_fail(c, CBM_TEST_RESULT_UNSUPPORTED);
    return c->status == CBM_TEST_RESULT_OK;
}

bool tir_identifier(tir_context *c, tir_bytes b, bool suite) {
    if (!b.length || (suite && b.length > 65536))
        return tir_fail(c, CBM_TEST_RESULT_UNSUPPORTED);
    for (size_t i = 0; i < b.length; i++) {
        if (!tir_step(c, 1, 0))
            return false;
        unsigned ch = b.data[i];
        bool letter = ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
        if (!letter && !(i && ch >= '0' && ch <= '9'))
            return tir_fail(c, CBM_TEST_RESULT_UNSUPPORTED);
    }
    return true;
}

int tir_compare(tir_context *c, tir_bytes a, tir_bytes b) {
    size_t common = a.length < b.length ? a.length : b.length;
    for (size_t i = 0; i < common; i++) {
        if (!tir_step(c, 1, 0))
            return 0;
        if (a.data[i] != b.data[i])
            return a.data[i] < b.data[i] ? -1 : 1;
    }
    return (a.length > b.length) - (a.length < b.length);
}

typedef struct {
    unsigned char *rows, *scratch;
    size_t size, middle, end;
    tir_compare_fn compare;
} tir_merge;

static bool tir_merge_part(tir_context *c, tir_merge *m, size_t left) {
    size_t a = left, b = m->middle;
    for (size_t dest = left; dest < m->end; dest++) {
        if (!tir_step(c, 0, 1))
            return false;
        bool take_a = b == m->end;
        if (!take_a && a < m->middle)
            take_a = m->compare(c, m->rows + a * m->size, m->rows + b * m->size) <= 0;
        size_t source = take_a ? a++ : b++;
        if (!tir_copy(c, m->scratch + dest * m->size, m->rows + source * m->size, m->size))
            return false;
    }
    return true;
}

bool tir_sort(tir_context *c, void *rows, size_t count, size_t size, tir_compare_fn compare) {
    if (count < 2)
        return c->status == CBM_TEST_RESULT_OK;
    unsigned char *scratch = tir_alloc(c, count, size);
    if (!scratch)
        return false;
    for (size_t width = 1; width < count;) {
        for (size_t left = 0; left < count;) {
            size_t middle = left + (count - left < width ? count - left : width);
            size_t end = middle + (count - middle < width ? count - middle : width);
            tir_merge merge = {rows, scratch, size, middle, end, compare};
            if (!tir_merge_part(c, &merge, left))
                return false;
            left = end;
        }
        if (!tir_copy(c, rows, scratch, count * size))
            return false;
        if (width >= count - width)
            break;
        width *= 2;
    }
    return true;
}
