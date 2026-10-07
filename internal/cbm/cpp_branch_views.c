/*
 * cpp_branch_views.c — one parse view per #if/#else branch (see the header).
 */
#include "cpp_branch_views.h"

#include <string.h>

enum { CPP_LINE_CODE, CPP_LINE_START, CPP_LINE_ELSE, CPP_LINE_END };

enum { CPP_NEST_MAX = 64 };

bool cbm_lang_needs_cpp_branch_views(CBMLanguage lang) {
    return lang == CBM_LANG_HASKELL;
}

static bool cpp_word_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

// Classify the line at src[pos..end): a conditional directive is a '#' in
// column 0, optional blanks, then the directive word — the same lines the
// Haskell scanner treats as conditionals.
static int cpp_line_kind(const char *src, int pos, int end) {
    if (pos >= end || src[pos] != '#') {
        return CPP_LINE_CODE;
    }
    int p = pos + 1;
    while (p < end && (src[p] == ' ' || src[p] == '\t')) {
        p++;
    }
    int w = p;
    while (w < end && cpp_word_char(src[w])) {
        w++;
    }
    static const struct {
        const char *word;
        int kind;
    } words[] = {
        {"if", CPP_LINE_START},      {"ifdef", CPP_LINE_START}, {"ifndef", CPP_LINE_START},
        {"else", CPP_LINE_ELSE},     {"elif", CPP_LINE_ELSE},   {"elifdef", CPP_LINE_ELSE},
        {"elifndef", CPP_LINE_ELSE}, {"endif", CPP_LINE_END},
    };
    size_t n = (size_t)(w - p);
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++) {
        if (strlen(words[i].word) == n && memcmp(src + p, words[i].word, n) == 0) {
            return words[i].kind;
        }
    }
    return CPP_LINE_CODE;
}

static int cpp_line_end(const char *src, int pos, int len) {
    const char *nl = memchr(src + pos, '\n', (size_t)(len - pos));
    return nl ? (int)(nl - src) : len;
}

typedef struct {
    int group;  // groups are numbered in order of their #if line
    int branch; // branch the current line is in
    bool outer; // the enclosing context is kept
} cpp_frame_t;

typedef struct {
    cpp_frame_t frames[CPP_NEST_MAX];
    int depth;
    int overflow; // groups nested past CPP_NEST_MAX are not tracked
    int next;
} cpp_nest_t;

// Track one line. Returns the innermost tracked group when the line is one of
// its #else/#elif lines, else NULL.
static cpp_frame_t *cpp_nest_step(cpp_nest_t *n, int kind, bool active) {
    switch (kind) {
    case CPP_LINE_START:
        if (n->depth < CPP_NEST_MAX && n->overflow == 0) {
            n->frames[n->depth++] = (cpp_frame_t){n->next, 0, active};
        } else {
            n->overflow++;
        }
        n->next++;
        return NULL;
    case CPP_LINE_ELSE:
        if (n->overflow > 0 || n->depth == 0) {
            return NULL;
        }
        n->frames[n->depth - 1].branch++;
        return &n->frames[n->depth - 1];
    case CPP_LINE_END:
        if (n->overflow > 0) {
            n->overflow--;
        } else if (n->depth > 0) {
            n->depth--;
        }
        return NULL;
    default:
        return NULL;
    }
}

// Branch count per group. Returns the group count, or -1 on allocation failure.
static int cpp_group_branches(CBMArena *a, const char *src, int len, int **out) {
    int groups = 0;
    for (int pos = 0; pos < len;) {
        int end = cpp_line_end(src, pos, len);
        groups += cpp_line_kind(src, pos, end) == CPP_LINE_START;
        pos = end + 1;
    }
    *out = NULL;
    if (groups == 0) {
        return 0;
    }
    int *branches = cbm_arena_alloc(a, sizeof(int) * (size_t)groups);
    if (!branches) {
        return -1;
    }
    for (int g = 0; g < groups; g++) {
        branches[g] = 1;
    }
    cpp_nest_t nest = {.depth = 0};
    for (int pos = 0; pos < len;) {
        int end = cpp_line_end(src, pos, len);
        cpp_frame_t *frame = cpp_nest_step(&nest, cpp_line_kind(src, pos, end), true);
        if (frame) {
            branches[frame->group] = frame->branch + 1;
        }
        pos = end + 1;
    }
    *out = branches;
    return groups;
}

int cbm_cpp_branch_view_count(CBMArena *a, const char *src, int len) {
    int *branches = NULL;
    int groups = cpp_group_branches(a, src, len, &branches);
    int most = 1;
    for (int g = 0; g < groups; g++) {
        if (branches[g] > most) {
            most = branches[g];
        }
    }
    int views = most - 1;
    return views > CBM_CPP_BRANCH_VIEWS_MAX ? CBM_CPP_BRANCH_VIEWS_MAX : views;
}

// The current line belongs to the view: every enclosing group sits in the
// branch the view keeps.
static bool cpp_view_keeps(const cpp_nest_t *n, const int *branches, int view) {
    if (n->depth == 0) {
        return true;
    }
    const cpp_frame_t *top = &n->frames[n->depth - 1];
    int nb = branches[top->group];
    int kept = 0;
    if (nb >= 2) {
        kept = view < nb - 1 ? view : nb - 1;
    }
    return top->outer && top->branch == kept;
}

char *cbm_cpp_branch_view(CBMArena *a, const char *src, int len, int view) {
    int *branches = NULL;
    int groups = cpp_group_branches(a, src, len, &branches);
    char *buf = groups >= 0 ? cbm_arena_alloc(a, (size_t)len + 1) : NULL;
    if (!buf) {
        return NULL;
    }
    memcpy(buf, src, (size_t)len);
    buf[len] = '\0';
    cpp_nest_t nest = {.depth = 0};
    for (int pos = 0; pos < len;) {
        int end = cpp_line_end(src, pos, len);
        bool kept = cpp_view_keeps(&nest, branches, view);
        int kind = cpp_line_kind(src, pos, end);
        cpp_nest_step(&nest, kind, kept);
        if (kind != CPP_LINE_CODE || !kept) {
            for (int i = pos; i < end; i++) {
                buf[i] = buf[i] == '\r' ? '\r' : ' ';
            }
        }
        pos = end + 1;
    }
    return buf;
}
