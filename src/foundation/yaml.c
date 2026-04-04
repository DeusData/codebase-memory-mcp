/*
 * yaml.c — Minimal YAML parser for config files.
 *
 * Strategy: line-by-line parsing with indentation tracking.
 * Each line is either:
 *   - Empty or comment (#) → skip
 *   - "key: value" → scalar entry in current map
 *   - "key:" (no value) → start of nested map or list
 *   - "- value" → list item
 *
 * Indentation determines nesting depth. Uses a stack of parent nodes.
 */
#include "foundation/constants.h"

enum { YAML_INIT_CAP = 8, YAML_LIST_PREFIX = 2, YAML_ROOT_INDENT = -1 };
#include "foundation/yaml.h"

#include <ctype.h>
#include <stddef.h> // NULL
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Node types ───────────────────────────────────────────────── */

typedef enum {
    YAML_MAP,
    YAML_LIST,
    YAML_SCALAR,
} yaml_type_t;

struct cbm_yaml_node {
    yaml_type_t type;
    char *key;   /* NULL for list items and root */
    char *value; /* scalar value (NULL for map/list) */

    /* Children (for map and list nodes) */
    cbm_yaml_node_t **children;
    int child_count;
    int child_cap;
};

/* ── Node lifecycle ───────────────────────────────────────────── */

static cbm_yaml_node_t *node_new(yaml_type_t type) {
    cbm_yaml_node_t *n = calloc(CBM_ALLOC_ONE, sizeof(*n));
    if (n) {
        n->type = type;
    }
    return n;
}

static bool node_add_child(cbm_yaml_node_t *parent, cbm_yaml_node_t *child) {
    if (!parent || !child) {
        return false;
    }
    if (parent->child_count >= parent->child_cap) {
        int new_cap =
            parent->child_cap < YAML_INIT_CAP ? YAML_INIT_CAP : parent->child_cap * PAIR_LEN;
        cbm_yaml_node_t **new_arr = realloc(parent->children, (size_t)new_cap * sizeof(*new_arr));
        if (!new_arr) {
            return false;
        }
        parent->children = new_arr;
        parent->child_cap = new_cap;
    }
    parent->children[parent->child_count++] = child;
    return true;
}

void cbm_yaml_free(cbm_yaml_node_t *root) {
    if (!root) {
        return;
    }
    enum { YAML_FREE_STACK = 256 };
    cbm_yaml_node_t *stack[YAML_FREE_STACK];
    int top = 0;
    stack[top++] = root;
    while (top > 0) {
        cbm_yaml_node_t *node = stack[--top];
        for (int i = 0; i < node->child_count && top < YAML_FREE_STACK; i++) {
            if (node->children[i]) {
                stack[top++] = node->children[i];
            }
        }
        free(node->children);
        free(node->key);
        free(node->value);
        free(node);
    }
}

/* ── String helpers ───────────────────────────────────────────── */

/* Duplicate a substring [start, end). Trims trailing whitespace. */
static char *trim_dup(const char *start, const char *end) {
    while (end > start && isspace((unsigned char)end[-SKIP_ONE])) {
        end--;
    }
    while (start < end && isspace((unsigned char)*start)) {
        start++;
    }
    if (start >= end) {
        return strdup("");
    }
    size_t len = (size_t)(end - start);
    char *s = malloc(len + SKIP_ONE);
    if (!s) {
        return NULL;
    }
    memcpy(s, start, len);
    s[len] = '\0';
    return s;
}

/* Count leading spaces (not tabs). */
static int leading_spaces(const char *line) {
    int n = 0;
    while (line[n] == ' ') {
        n++;
    }
    return n;
}

/* ── Parser ───────────────────────────────────────────────────── */

/* Stack entry for tracking parent context during parsing. */
typedef struct {
    cbm_yaml_node_t *node;
    int indent;
} stack_entry_t;

/* Parse a list item "- value" and add to the correct parent. */
static void parse_list_item(const char *content, int content_len, cbm_yaml_node_t *parent) {
    const char *item_start = content + YAML_LIST_PREFIX;
    int item_len = content_len - YAML_LIST_PREFIX;
    while (item_len > 0 && isspace((unsigned char)item_start[0])) {
        item_start++;
        item_len--;
    }

    /* If parent is a map, the list items belong to the last child */
    cbm_yaml_node_t *list_parent = parent;
    if (parent->type == YAML_MAP && parent->child_count > 0) {
        cbm_yaml_node_t *last = parent->children[parent->child_count - SKIP_ONE];
        if (last->type == YAML_LIST) {
            list_parent = last;
        }
    }

    cbm_yaml_node_t *item = node_new(YAML_SCALAR);
    if (item) {
        item->value = trim_dup(item_start, item_start + item_len);
        if (!node_add_child(list_parent, item)) {
            cbm_yaml_free(item);
        }
    }
}

/* Strip inline comments from a value string (not inside quotes).
 * Returns the adjusted length. */
static int strip_inline_comment(const char *after, int after_len) {
    if (after_len > 0 && after[0] != '"' && after[0] != '\'') {
        for (int i = 0; i < after_len; i++) {
            if (after[i] == '#' && i > 0 && after[i - SKIP_ONE] == ' ') {
                after_len = i;
                while (after_len > 0 && isspace((unsigned char)after[after_len - SKIP_ONE])) {
                    after_len--;
                }
                break;
            }
        }
    }
    return after_len;
}

/* Peek ahead to check if next content line is a list item. */
static bool peek_is_list(const char *eol, const char *end) {
    const char *peek = (eol < end) ? eol + SKIP_ONE : end;
    while (peek < end) {
        const char *peek_eol = memchr(peek, '\n', (size_t)(end - peek));
        if (!peek_eol) {
            peek_eol = end;
        }
        int pi = leading_spaces(peek);
        const char *pc = peek + pi;
        int pcl = (int)(peek_eol - pc);
        if (pcl > 0 && pc[pcl - SKIP_ONE] == '\r') {
            pcl--;
        }
        if (pcl == 0 || pc[0] == '#') {
            peek = (peek_eol < end) ? peek_eol + SKIP_ONE : end;
            continue;
        }
        return pc[0] == '-';
    }
    return false;
}

/* Parse a "key: value" or "key:" line. */
static void parse_key_line(const char *content, int content_len, int indent,
                           cbm_yaml_node_t *parent, stack_entry_t *stack, int *stack_depth,
                           const char *eol, const char *end) {
    const char *colon = memchr(content, ':', (size_t)content_len);
    if (!colon) {
        return;
    }

    char *key = trim_dup(content, colon);
    if (!key) {
        return;
    }

    /* After colon: value or nothing */
    const char *after = colon + SKIP_ONE;
    int after_len = content_len - (int)(after - content);
    while (after_len > 0 && isspace((unsigned char)*after)) {
        after++;
        after_len--;
    }

    after_len = strip_inline_comment(after, after_len);

    if (after_len > 0) {
        /* "key: value" — scalar */
        cbm_yaml_node_t *child = node_new(YAML_SCALAR);
        if (child) {
            child->key = key;
            child->value = trim_dup(after, after + after_len);
            if (!node_add_child(parent, child)) {
                cbm_yaml_free(child);
            }
        } else {
            free(key);
        }
    } else {
        /* "key:" — could be map or list (determined by next lines) */
        bool is_list = peek_is_list(eol, end);
        cbm_yaml_node_t *child = node_new(is_list ? YAML_LIST : YAML_MAP);
        if (child) {
            child->key = key;
            if (!node_add_child(parent, child)) {
                cbm_yaml_free(child);
            } else if (*stack_depth < CBM_SZ_32) {
                stack[(*stack_depth)++] = (stack_entry_t){.node = child, .indent = indent};
            }
        } else {
            free(key);
        }
    }
}

cbm_yaml_node_t *cbm_yaml_parse(const char *text, int len) {
    if (!text || len <= 0) {
        return node_new(YAML_MAP);
    }

    cbm_yaml_node_t *root = node_new(YAML_MAP);
    if (!root) {
        return NULL;
    }

    /* Stack for tracking parent context */
    stack_entry_t stack[CBM_SZ_32];
    int stack_depth = 0;
    stack[0] = (stack_entry_t){.node = root, .indent = YAML_ROOT_INDENT};
    stack_depth = SKIP_ONE;

    const char *p = text;
    const char *end = text + len;

    while (p < end) {
        /* Find end of line */
        const char *eol = memchr(p, '\n', (size_t)(end - p));
        if (!eol) {
            eol = end;
        }
        int line_len = (int)(eol - p);

        /* Skip empty lines and comments */
        int indent = leading_spaces(p);
        const char *content = p + indent;
        int content_len = line_len - indent;

        /* Strip \r */
        if (content_len > 0 && content[content_len - SKIP_ONE] == '\r') {
            content_len--;
        }

        if (content_len == 0 || content[0] == '#') {
            p = (eol < end) ? eol + SKIP_ONE : end;
            continue;
        }

        /* Pop stack to find parent at correct indentation */
        while (stack_depth > SKIP_ONE && stack[stack_depth - SKIP_ONE].indent >= indent) {
            stack_depth--;
        }
        cbm_yaml_node_t *parent = stack[stack_depth - SKIP_ONE].node;

        if (content[0] == '-' && content_len >= PAIR_LEN && content[SKIP_ONE] == ' ') {
            parse_list_item(content, content_len, parent);
        } else {
            parse_key_line(content, content_len, indent, parent, stack, &stack_depth, eol, end);
        }

        p = (eol < end) ? eol + SKIP_ONE : end;
    }

    return root;
}

/* ── Query helpers ────────────────────────────────────────────── */

/* Find a child node by key in a map node. */
static const cbm_yaml_node_t *find_child(const cbm_yaml_node_t *node, const char *key) {
    if (!node || !key) {
        return NULL;
    }
    for (int i = 0; i < node->child_count; i++) {
        if (node->children[i]->key && strcmp(node->children[i]->key, key) == 0) {
            return node->children[i];
        }
    }
    return NULL;
}

/* Navigate to a node by dot-separated path. */
static const cbm_yaml_node_t *navigate(const cbm_yaml_node_t *root, const char *path) {
    if (!root || !path) {
        return NULL;
    }

    const cbm_yaml_node_t *cur = root;
    char buf[CBM_SZ_256];
    const char *p = path;

    while (*p) {
        const char *dot = strchr(p, '.');
        int seg_len;
        if (dot) {
            seg_len = (int)(dot - p);
        } else {
            seg_len = (int)strlen(p);
        }
        if (seg_len <= 0 || seg_len >= (int)sizeof(buf)) {
            return NULL;
        }

        memcpy(buf, p, (size_t)seg_len);
        buf[seg_len] = '\0';

        cur = find_child(cur, buf);
        if (!cur) {
            return NULL;
        }

        p += seg_len;
        if (*p == '.') {
            p++;
        }
    }

    return cur;
}

const char *cbm_yaml_get_str(const cbm_yaml_node_t *root, const char *path) {
    const cbm_yaml_node_t *node = navigate(root, path);
    if (!node || node->type != YAML_SCALAR) {
        return NULL;
    }
    return node->value;
}

double cbm_yaml_get_float(const cbm_yaml_node_t *root, const char *path, double default_val) {
    const char *str = cbm_yaml_get_str(root, path);
    if (!str) {
        return default_val;
    }
    char *endptr;
    double val = strtod(str, &endptr);
    if (endptr == str) {
        return default_val;
    }
    return val;
}

bool cbm_yaml_get_bool(const cbm_yaml_node_t *root, const char *path, bool default_val) {
    const char *str = cbm_yaml_get_str(root, path);
    if (!str) {
        return default_val;
    }

    if (strcasecmp(str, "true") == 0 || strcasecmp(str, "yes") == 0 || strcasecmp(str, "on") == 0 ||
        strcmp(str, "1") == 0) {
        return true;
    }
    if (strcasecmp(str, "false") == 0 || strcasecmp(str, "no") == 0 ||
        strcasecmp(str, "off") == 0 || strcmp(str, "0") == 0) {
        return false;
    }
    return default_val;
}

int cbm_yaml_get_str_list(const cbm_yaml_node_t *root, const char *path, const char **out,
                          int max_out) {
    const cbm_yaml_node_t *node = navigate(root, path);
    if (!node || node->type != YAML_LIST) {
        return 0;
    }

    int count = 0;
    for (int i = 0; i < node->child_count && count < max_out; i++) {
        if (node->children[i]->type == YAML_SCALAR && node->children[i]->value) {
            out[count++] = node->children[i]->value;
        }
    }
    return count;
}

bool cbm_yaml_has(const cbm_yaml_node_t *root, const char *path) {
    return navigate(root, path) != NULL;
}
