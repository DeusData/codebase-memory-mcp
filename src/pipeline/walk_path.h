/*
 * walk_path.h — Checked reusable path storage for exact pipeline walks.
 *
 * A walker keeps one absolute and one relative instance, appends an entry,
 * then restores the saved parent length. Geometric growth makes all reserve
 * copying across a walk O(P), where P is the longest path in bytes; live path
 * memory is O(P). Capacity is an allocation detail, never a traversal limit.
 */
#ifndef CBM_PIPELINE_WALK_PATH_H
#define CBM_PIPELINE_WALK_PATH_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum { CBM_WALK_PATH_GROWTH_FACTOR = 2 };

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} cbm_walk_path_t;

static inline bool cbm_walk_path_size_add(size_t *total, size_t amount) {
    if (!total || amount > SIZE_MAX - *total) {
        return false;
    }
    *total += amount;
    return true;
}

static inline bool cbm_walk_path_init(cbm_walk_path_t *path, const char *initial) {
    if (!path || !initial) {
        return false;
    }
    memset(path, 0, sizeof(*path));
    size_t length = strlen(initial);
    size_t capacity = length;
    if (!cbm_walk_path_size_add(&capacity, 1U)) {
        return false;
    }
    path->data = malloc(capacity);
    if (!path->data) {
        return false;
    }
    memcpy(path->data, initial, length + 1U);
    path->length = length;
    path->capacity = capacity;
    return true;
}

static inline bool cbm_walk_path_reserve(cbm_walk_path_t *path, size_t required) {
    if (!path || !path->data) {
        return false;
    }
    if (required <= path->capacity) {
        return true;
    }
    size_t capacity = path->capacity;
    while (capacity < required) {
        if (capacity > SIZE_MAX / CBM_WALK_PATH_GROWTH_FACTOR) {
            capacity = required;
            break;
        }
        capacity *= CBM_WALK_PATH_GROWTH_FACTOR;
    }
    char *grown = realloc(path->data, capacity);
    if (!grown) {
        return false;
    }
    path->data = grown;
    path->capacity = capacity;
    return true;
}

static inline bool cbm_walk_path_append(cbm_walk_path_t *path, const char *name) {
    if (!path || !path->data || !name) {
        return false;
    }
    size_t separator_length = path->length > 0 ? 1U : 0U;
    size_t required = path->length;
    size_t name_length = strlen(name);
    if (!cbm_walk_path_size_add(&required, separator_length) ||
        !cbm_walk_path_size_add(&required, name_length) || !cbm_walk_path_size_add(&required, 1U) ||
        !cbm_walk_path_reserve(path, required)) {
        return false;
    }
    size_t offset = path->length;
    if (separator_length > 0) {
        path->data[offset++] = '/';
    }
    memcpy(path->data + offset, name, name_length);
    path->length = offset + name_length;
    path->data[path->length] = '\0';
    return true;
}

static inline void cbm_walk_path_restore(cbm_walk_path_t *path, size_t length) {
    if (path && path->data && length <= path->length) {
        path->length = length;
        path->data[length] = '\0';
    }
}

static inline void cbm_walk_path_free(cbm_walk_path_t *path) {
    if (!path) {
        return;
    }
    free(path->data);
    memset(path, 0, sizeof(*path));
}

#endif /* CBM_PIPELINE_WALK_PATH_H */
