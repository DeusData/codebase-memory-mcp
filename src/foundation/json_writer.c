/* Strict JSON output for documents that can carry source text. */
#include "foundation/json_writer.h"

#include "foundation/str_util.h"

static bool json_clean_string(yyjson_mut_doc *doc, yyjson_mut_val *value) {
    const char *text = yyjson_mut_get_str(value);
    size_t length = yyjson_mut_get_len(value);
    if (!text || cbm_utf8_valid(text, length))
        return true;
    size_t clean_length = 0;
    char *clean = cbm_utf8_lossy_dup(CBM_MEM_CLASS_OTHER, text, length, &clean_length);
    yyjson_mut_val *owned = clean ? yyjson_mut_strncpy(doc, clean, clean_length) : NULL;
    cbm_free(CBM_MEM_CLASS_OTHER, clean);
    return owned && yyjson_mut_set_strn(value, yyjson_mut_get_str(owned), clean_length);
}

/* Room for the children of the next container, on the walk's explicit stack. */
static bool json_stack_reserve(yyjson_mut_val ***stack, size_t *capacity, size_t needed) {
    if (needed <= *capacity)
        return true;
    size_t grown = *capacity * 2 > needed ? *capacity * 2 : needed;
    yyjson_mut_val **next = cbm_realloc(CBM_MEM_CLASS_OTHER, *stack, grown * sizeof(*next));
    if (!next)
        return false;
    *stack = next;
    *capacity = grown;
    return true;
}

static bool json_clean_strings(yyjson_mut_doc *doc, yyjson_mut_val *root) {
    size_t capacity = 16, top = 1;
    yyjson_mut_val **stack = cbm_alloc(CBM_MEM_CLASS_OTHER, capacity * sizeof(*stack));
    if (!stack)
        return false;
    stack[0] = root;
    bool ok = true;
    while (ok && top) {
        yyjson_mut_val *value = stack[--top];
        size_t index, count;
        yyjson_mut_val *key, *item;
        if (yyjson_mut_is_str(value)) {
            ok = json_clean_string(doc, value);
        } else if (yyjson_mut_is_arr(value)) {
            ok = json_stack_reserve(&stack, &capacity, top + yyjson_mut_arr_size(value));
            if (ok) {
                yyjson_mut_arr_foreach(value, index, count, item) {
                    stack[top++] = item;
                }
            }
        } else if (yyjson_mut_is_obj(value)) {
            ok = json_stack_reserve(&stack, &capacity, top + yyjson_mut_obj_size(value));
            if (ok) {
                yyjson_mut_obj_foreach(value, index, count, key, item) {
                    ok = ok && json_clean_string(doc, key);
                    stack[top++] = item;
                }
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, stack);
    return ok;
}

char *cbm_json_val_write_text(yyjson_mut_doc *doc, yyjson_mut_val *value, const yyjson_alc *alc,
                              size_t *length) {
    yyjson_write_err error = {0};
    char *json = yyjson_mut_val_write_opts(value, 0, alc, length, &error);
    if (json || error.code != YYJSON_WRITE_ERROR_INVALID_STRING)
        return json;
    return json_clean_strings(doc, value) ? yyjson_mut_val_write_opts(value, 0, alc, length, NULL)
                                          : NULL;
}

char *cbm_json_write_text(yyjson_mut_doc *doc, const yyjson_alc *alc, size_t *length) {
    return cbm_json_val_write_text(doc, yyjson_mut_doc_get_root(doc), alc, length);
}
