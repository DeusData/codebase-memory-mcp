/* URL constant folding for HTTP client calls (issues #706, #1147).
 * See extract_url_fold.h for the contract. */
#include "extract_url_fold.h"
#include "cbm.h"              // CBMLanguage
#include "foundation/arena.h" // cbm_arena_strdup, cbm_arena_strndup, cbm_arena_sprintf
#include "helpers.h"
#include "foundation/constants.h"
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* FOLD_BUF matches the template flattener's limit; FOLD_MAX_DEPTH bounds the
 * recursion of nested `a + b + c` chains and templates; object-literal endpoint
 * maps are followed at most FOLD_MAX_OBJ_DEPTH levels deep. */
enum { FOLD_BUF = 512, FOLD_MAX_DEPTH = 16, FOLD_MAX_OBJ_DEPTH = 4, FOLD_NAME_BUF = 256 };

static const char k_unknown[] = "{}";

bool cbm_url_fold_lang(CBMLanguage lang) {
    return lang == CBM_LANG_PYTHON || lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT ||
           lang == CBM_LANG_TSX;
}

static bool at_placeholder(const char *s) {
    return s[0] == '{' && s[SKIP_ONE] == '}';
}

bool cbm_url_has_literal_path(const char *url) {
    if (!url) {
        return false;
    }
    const char *p = url;
    while (*p) {
        if (at_placeholder(p)) {
            p += PAIR_LEN;
        } else if (*p == '/') {
            p++;
        } else {
            return true;
        }
    }
    return false;
}

/* Any text at all besides "{}" placeholders ("/" counts: a root constant). */
static bool has_literal_text(const char *s) {
    const char *p = s;
    while (*p) {
        if (!at_placeholder(p)) {
            return true;
        }
        p += PAIR_LEN;
    }
    return false;
}

/* "some/path", "api/users/{}": a relative URL path of at least two segments.
 * Only such a value is read as a path under a base URL. */
static bool relative_path_shaped(const char *s) {
    if (!isalnum((unsigned char)s[0]) && s[0] != '_') {
        return false;
    }
    bool slash = false;
    bool segment_after_slash = false;
    for (const char *p = s; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '/') {
            if (p[SKIP_ONE] == '/') {
                return false;
            }
            slash = true;
            continue;
        }
        if (!isalnum(c) && !strchr("-_.~%{}", c)) {
            return false;
        }
        segment_after_slash = segment_after_slash || slash;
    }
    return slash && segment_after_slash;
}

static const char *lookup_constant(const CBMExtractCtx *ctx, const char *name, bool url) {
    const CBMStringConstantMap *map = &ctx->string_constants;
    for (int i = 0; i < map->count; i++) {
        if (!map->is_url_builder[i] && map->values[i] && strcmp(map->names[i], name) == 0) {
            return url && map->url_values[i] ? map->url_values[i] : map->values[i];
        }
    }
    return NULL;
}

static void push_constant(CBMExtractCtx *ctx, const char *name, const char *value,
                          const char *url_value) {
    CBMStringConstantMap *map = &ctx->string_constants;
    if (!name || !value || map->count >= CBM_MAX_STRING_CONSTANTS) {
        return;
    }
    map->names[map->count] = name;
    map->values[map->count] = value;
    map->url_values[map->count] = url_value;
    map->is_url_builder[map->count] = false;
    map->count++;
}

/* --- folding --------------------------------------------------------- */

typedef struct {
    char text[FOLD_BUF];
    size_t len;
    bool overflow;
    bool url;       /* URL projection; false preserves raw topic/string identity */
    bool base_ref;  /* the first part is a resolved constant reference */
    bool has_parts; /* at least one part appended */
} fold_buf_t;

static const char *fold_node(CBMExtractCtx *ctx, TSNode node, int depth, bool url);

static void buf_append(fold_buf_t *b, const char *piece) {
    /* Only URL projections collapse the boundary between a base ending in '/'
     * and a path starting with '/'. Raw strings/topics retain both separators
     * ("tenant/" + "/events" is "tenant//events"). Avoid doubling a separator ("{}/" + "/api/x"); a
     * scheme's "://" is kept. */
    if (b->url && b->len > 0 && b->text[b->len - SKIP_ONE] == '/' && piece[0] == '/' &&
        !(b->len >= PAIR_LEN && b->text[b->len - PAIR_LEN] == ':')) {
        piece++;
    }
    size_t pl = strlen(piece);
    if (b->len + pl >= FOLD_BUF) {
        b->overflow = true;
        return;
    }
    memcpy(b->text + b->len, piece, pl);
    b->len += pl;
    b->text[b->len] = '\0';
}

static bool is_reference_kind(const char *k) {
    return strcmp(k, "identifier") == 0 || strcmp(k, "member_expression") == 0 ||
           strcmp(k, "template_substitution") == 0 || strcmp(k, "interpolation") == 0;
}

/* Append one part of a composition. The value of a template substitution or
 * f-string interpolation is its inner expression. */
static void fold_part(CBMExtractCtx *ctx, fold_buf_t *b, TSNode part, int depth) {
    const char *k = ts_node_type(part);
    TSNode expr = part;
    if (strcmp(k, "template_substitution") == 0) {
        expr = ts_node_named_child(part, 0);
    } else if (strcmp(k, "interpolation") == 0) {
        expr = ts_node_child_by_field_name(part, TS_FIELD("expression"));
    }
    const char *v = ts_node_is_null(expr) ? NULL : fold_node(ctx, expr, depth + SKIP_ONE, b->url);
    if (!b->has_parts) {
        b->base_ref = v && is_reference_kind(k) && !at_placeholder(v);
    }
    b->has_parts = true;
    buf_append(b, v ? v : k_unknown);
}

static void fold_literal_piece(CBMExtractCtx *ctx, fold_buf_t *b, TSNode piece) {
    const char *t = cbm_node_text(ctx->arena, piece, ctx->source);
    b->has_parts = true;
    buf_append(b, t ? t : "");
}

/* In URL projections, a resolved first constant is `BASE + path`: a
 * relative BASE ('api') joined to a path is a path under the client's base
 * URL, so it is read from the root ("/api/users/me"). */
static const char *finish(CBMExtractCtx *ctx, const fold_buf_t *b) {
    if (b->overflow) {
        return NULL;
    }
    if (b->url && b->base_ref && b->text[0] != '/' && !strstr(b->text, "://") &&
        relative_path_shaped(b->text)) {
        return cbm_arena_sprintf(ctx->arena, "/%s", b->text);
    }
    return cbm_arena_strndup(ctx->arena, b->text, b->len);
}

/* JS/TS `string`, Python `string` (also f-strings), JS `template_string`. */
static const char *fold_string_like(CBMExtractCtx *ctx, TSNode node, int depth, bool url) {
    fold_buf_t b = {.len = 0, .url = url};
    b.text[0] = '\0';
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(node, i);
        const char *k = ts_node_type(c);
        if (strcmp(k, "template_substitution") == 0 || strcmp(k, "interpolation") == 0) {
            fold_part(ctx, &b, c, depth);
        } else if (strcmp(k, "string_fragment") == 0 || strcmp(k, "string_content") == 0 ||
                   strcmp(k, "escape_sequence") == 0) {
            fold_literal_piece(ctx, &b, c);
        } else if (strcmp(k, "escape_interpolation") == 0) {
            b.has_parts = true;
            buf_append(&b, "{"); /* `{{` in an f-string is one literal brace */
        }
    }
    return finish(ctx, &b);
}

static bool is_plus(CBMExtractCtx *ctx, TSNode bin) {
    TSNode op = ts_node_child_by_field_name(bin, TS_FIELD("operator"));
    if (ts_node_is_null(op)) {
        return false;
    }
    const char *t = cbm_node_text(ctx->arena, op, ctx->source);
    return t && strcmp(t, "+") == 0;
}

/* `a + b` (JS binary_expression / Python binary_operator). */
static const char *fold_concat(CBMExtractCtx *ctx, TSNode node, int depth, bool url) {
    if (!is_plus(ctx, node)) {
        return NULL;
    }
    TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
    TSNode right = ts_node_child_by_field_name(node, TS_FIELD("right"));
    if (ts_node_is_null(left) || ts_node_is_null(right)) {
        return NULL;
    }
    fold_buf_t b = {.len = 0, .url = url};
    b.text[0] = '\0';
    fold_part(ctx, &b, left, depth);
    fold_part(ctx, &b, right, depth);
    return finish(ctx, &b);
}

/* Python implicit concatenation: "a" "b". */
static const char *fold_implicit_concat(CBMExtractCtx *ctx, TSNode node, int depth, bool url) {
    fold_buf_t b = {.len = 0, .url = url};
    b.text[0] = '\0';
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        fold_part(ctx, &b, ts_node_named_child(node, i), depth);
    }
    return finish(ctx, &b);
}

/* "A.B.C" for a JS member chain over identifiers, or false. */
static bool dotted_name(CBMExtractCtx *ctx, TSNode node, char *out, size_t cap, int depth) {
    const char *k = ts_node_type(node);
    if (strcmp(k, "identifier") == 0) {
        const char *t = cbm_node_text(ctx->arena, node, ctx->source);
        return t && snprintf(out, cap, "%s", t) < (int)cap;
    }
    if (strcmp(k, "member_expression") != 0 || depth >= FOLD_MAX_OBJ_DEPTH) {
        return false;
    }
    TSNode obj = ts_node_child_by_field_name(node, TS_FIELD("object"));
    TSNode prop = ts_node_child_by_field_name(node, TS_FIELD("property"));
    if (ts_node_is_null(obj) || ts_node_is_null(prop) ||
        strcmp(ts_node_type(prop), "property_identifier") != 0 ||
        !dotted_name(ctx, obj, out, cap, depth + SKIP_ONE)) {
        return false;
    }
    const char *p = cbm_node_text(ctx->arena, prop, ctx->source);
    size_t used = strlen(out);
    return p && snprintf(out + used, cap - used, ".%s", p) < (int)(cap - used);
}

static const char *fold_reference(CBMExtractCtx *ctx, TSNode node, bool url) {
    char name[FOLD_NAME_BUF];
    if (!dotted_name(ctx, node, name, sizeof(name), 0)) {
        return NULL;
    }
    return lookup_constant(ctx, name, url);
}

/* Type-only wrappers and grouping: `x as const`, `x satisfies T`, `x!`, `(x)`. */
static bool is_transparent_wrapper(const char *k) {
    return strcmp(k, "parenthesized_expression") == 0 || strcmp(k, "as_expression") == 0 ||
           strcmp(k, "satisfies_expression") == 0 || strcmp(k, "non_null_expression") == 0;
}

static const char *fold_node(CBMExtractCtx *ctx, TSNode node, int depth, bool url) {
    if (depth > FOLD_MAX_DEPTH || ts_node_is_null(node)) {
        return NULL;
    }
    const char *k = ts_node_type(node);
    if (strcmp(k, "string") == 0 || strcmp(k, "template_string") == 0) {
        return fold_string_like(ctx, node, depth, url);
    }
    if (strcmp(k, "identifier") == 0 || strcmp(k, "member_expression") == 0) {
        return fold_reference(ctx, node, url);
    }
    if (strcmp(k, "binary_expression") == 0 || strcmp(k, "binary_operator") == 0) {
        return fold_concat(ctx, node, depth, url);
    }
    if (strcmp(k, "concatenated_string") == 0) {
        return fold_implicit_concat(ctx, node, depth, url);
    }
    if (is_transparent_wrapper(k) && ts_node_named_child_count(node) > 0) {
        return fold_node(ctx, ts_node_named_child(node, 0), depth + SKIP_ONE, url);
    }
    return NULL;
}

/* --- constants ------------------------------------------------------- */

/* Strip type wrappers and `Object.freeze(...)` from a constant's value. */
static TSNode unwrap_value(CBMExtractCtx *ctx, TSNode v) {
    for (int i = 0; i < FOLD_MAX_OBJ_DEPTH && !ts_node_is_null(v); i++) {
        const char *k = ts_node_type(v);
        if (is_transparent_wrapper(k) && ts_node_named_child_count(v) > 0) {
            v = ts_node_named_child(v, 0);
            continue;
        }
        if (strcmp(k, "call_expression") != 0) {
            break;
        }
        TSNode fn = ts_node_child_by_field_name(v, TS_FIELD("function"));
        TSNode args = ts_node_child_by_field_name(v, TS_FIELD("arguments"));
        const char *ft = ts_node_is_null(fn) ? NULL : cbm_node_text(ctx->arena, fn, ctx->source);
        if (!ft || strcmp(ft, "Object.freeze") != 0 || ts_node_is_null(args) ||
            ts_node_named_child_count(args) != SKIP_ONE) {
            break;
        }
        v = ts_node_named_child(args, 0);
    }
    return v;
}

static void record_value(CBMExtractCtx *ctx, const char *name, TSNode value, int depth);

/* The key of an object-literal pair as a plain name, or NULL. */
static const char *pair_key(CBMExtractCtx *ctx, TSNode pair) {
    TSNode key = ts_node_child_by_field_name(pair, TS_FIELD("key"));
    if (ts_node_is_null(key)) {
        return NULL;
    }
    const char *k = ts_node_type(key);
    if (strcmp(k, "property_identifier") == 0) {
        return cbm_node_text(ctx->arena, key, ctx->source);
    }
    if (strcmp(k, "string") == 0) {
        return fold_string_like(ctx, key, 0, false);
    }
    return NULL;
}

static void record_object(CBMExtractCtx *ctx, const char *prefix, TSNode obj, int depth) {
    uint32_t nc = ts_node_named_child_count(obj);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode member = ts_node_named_child(obj, i);
        const char *k = ts_node_type(member);
        if (strcmp(k, "pair") == 0) {
            const char *key = pair_key(ctx, member);
            TSNode value = ts_node_child_by_field_name(member, TS_FIELD("value"));
            if (key && key[0] && !ts_node_is_null(value)) {
                record_value(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, key), value,
                             depth + SKIP_ONE);
            }
        } else if (strcmp(k, "shorthand_property_identifier") == 0) {
            /* `{ BASE }` carries the constant of the same name. */
            const char *key = cbm_node_text(ctx->arena, member, ctx->source);
            const char *v = key ? lookup_constant(ctx, key, false) : NULL;
            if (v) {
                push_constant(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, key), v,
                              lookup_constant(ctx, key, true));
            }
        }
    }
}

static void record_value(CBMExtractCtx *ctx, const char *name, TSNode value, int depth) {
    if (!name || depth > FOLD_MAX_OBJ_DEPTH) {
        return;
    }
    value = unwrap_value(ctx, value);
    if (ts_node_is_null(value)) {
        return;
    }
    if (strcmp(ts_node_type(value), "object") == 0) {
        record_object(ctx, name, value, depth);
        return;
    }
    const char *v = fold_node(ctx, value, 0, false);
    if (v && has_literal_text(v)) {
        push_constant(ctx, name, v, fold_node(ctx, value, 0, true));
    }
}

void cbm_url_fold_record_constant(CBMExtractCtx *ctx, const char *name, TSNode value) {
    if (!name || !name[0] || ts_node_is_null(value)) {
        return;
    }
    record_value(ctx, name, value, 0);
}

/* --- call sites ------------------------------------------------------ */

/* Python strings fold only when they interpolate (an f-string); a plain
 * literal keeps the long-standing literal path. */
static bool is_interpolated_string(TSNode node) {
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        if (strcmp(ts_node_type(ts_node_named_child(node, i)), "interpolation") == 0) {
            return true;
        }
    }
    return false;
}

/* A Python string whose opening token is more than one quote -- a prefix
 * (r"", b"", u"", an f"" with no field) or a triple quote -- cannot go down
 * the literal path: that strips exactly one quote from each end, so the
 * prefix and the extra quotes stayed in the URL and it never matched a route
 * (#523). Folding reads the content pieces instead. JS/TS strings have no
 * string_start, so this is Python only. */
static bool is_wrapped_string(TSNode node) {
    TSNode start = ts_node_child(node, 0);
    return !ts_node_is_null(start) && strcmp(ts_node_type(start), "string_start") == 0 &&
           ts_node_end_byte(start) - ts_node_start_byte(start) > SKIP_ONE;
}

static bool is_foldable_arg(TSNode arg) {
    const char *k = ts_node_type(arg);
    if (strcmp(k, "string") == 0) {
        return is_interpolated_string(arg) || is_wrapped_string(arg);
    }
    return strcmp(k, "template_string") == 0 || strcmp(k, "binary_expression") == 0 ||
           strcmp(k, "binary_operator") == 0 || strcmp(k, "identifier") == 0 ||
           strcmp(k, "member_expression") == 0 || strcmp(k, "concatenated_string") == 0 ||
           is_transparent_wrapper(k);
}

/* The literal path after an unresolvable base, never the base: a rooted
 * path that names something literal, or a relative one read from the root.
 * `rest` must live in the arena; a rooted one is returned as is. */
static const char *path_after_unknown_base(CBMExtractCtx *ctx, const char *rest) {
    if (rest[0] == '/') {
        return cbm_url_has_literal_path(rest) ? rest : NULL;
    }
    if (relative_path_shaped(rest)) {
        return cbm_arena_sprintf(ctx->arena, "/%s", rest);
    }
    return NULL;
}

/* --- Rust URL helpers (#3115) ----------------------------------------- */

/* A Rust client that joins each path onto a runtime base URL
 * (`self.client.post(self.make_url("v2/users")?)`) names the path only in the
 * helper's argument. The list is short on purpose: folding any call's first
 * argument would turn `client.get(find_path("a/b"))` into an HTTP call, and
 * generic names such as `path_for` or `url_for` also build asset and file
 * paths, which would mint routes that do not exist. */
static const char *const k_url_builders[] = {"make_url", "build_url", NULL};

static bool is_url_builder_call(CBMExtractCtx *ctx, TSNode node) {
    if (strcmp(ts_node_type(node), "call_expression") != 0) {
        return false;
    }
    TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
    if (ts_node_is_null(fn)) {
        return false;
    }
    const char *fk = ts_node_type(fn);
    if (strcmp(fk, "field_expression") == 0) { /* self.make_url */
        fn = ts_node_child_by_field_name(fn, TS_FIELD("field"));
    } else if (strcmp(fk, "scoped_identifier") == 0) { /* urls::make_url */
        fn = ts_node_child_by_field_name(fn, TS_FIELD("name"));
    } else if (strcmp(fk, "identifier") != 0) { /* make_url */
        return false;
    }
    if (ts_node_is_null(fn)) {
        return false;
    }
    /* Compared in place: this runs for every Rust call argument. */
    const char *name = ctx->source + ts_node_start_byte(fn);
    size_t len = ts_node_end_byte(fn) - ts_node_start_byte(fn);
    for (int i = 0; k_url_builders[i]; i++) {
        if (strlen(k_url_builders[i]) == len && memcmp(name, k_url_builders[i], len) == 0) {
            return true;
        }
    }
    return false;
}

/* The literal a URL helper receives, behind at most one `&`: a string literal,
 * or the literal format string of `format!("...", ..)` (*is_format set).
 * Anything else is a null node. */
static TSNode helper_literal(CBMExtractCtx *ctx, TSNode arg, bool *is_format) {
    *is_format = false;
    if (strcmp(ts_node_type(arg), "reference_expression") == 0) {
        arg = ts_node_child_by_field_name(arg, TS_FIELD("value"));
        if (ts_node_is_null(arg)) {
            return arg;
        }
    }
    const char *k = ts_node_type(arg);
    if (strcmp(k, "string_literal") == 0 || strcmp(k, "raw_string_literal") == 0) {
        return arg;
    }
    if (strcmp(k, "macro_invocation") == 0) {
        TSNode mac = ts_node_child_by_field_name(arg, TS_FIELD("macro"));
        TSNode tt = cbm_find_child_by_kind(arg, "token_tree");
        const char *m = ts_node_is_null(mac) ? NULL : cbm_node_text(ctx->arena, mac, ctx->source);
        TSNode first = ts_node_is_null(tt) ? tt : ts_node_named_child(tt, 0);
        if (m && strcmp(m, "format") == 0 && !ts_node_is_null(first) &&
            strcmp(ts_node_type(first), "string_literal") == 0) {
            *is_format = true;
            return first;
        }
    }
    return (TSNode){0};
}

/* A format string with every `{..}` argument as "{}" and `{{` / `}}` as one
 * literal brace. False on an unbalanced brace or overflow. */
static bool flatten_format(const char *raw, char *out, size_t cap) {
    size_t n = 0;
    for (const char *p = raw; *p; p++) {
        const char *piece = p;
        size_t len = SKIP_ONE;
        if ((p[0] == '{' || p[0] == '}') && p[SKIP_ONE] == p[0]) {
            p++;
        } else if (p[0] == '{') {
            const char *close = strchr(p, '}');
            if (!close) {
                return false;
            }
            piece = k_unknown;
            len = PAIR_LEN;
            p = close;
        } else if (p[0] == '}') {
            return false;
        }
        if (n + len >= cap) {
            return false;
        }
        memcpy(out + n, piece, len);
        n += len;
    }
    out[n] = '\0';
    return true;
}

/* The URL a Rust argument carries: only an allowlisted helper call, behind any
 * `?` or parentheses. The helper joins its literal onto a base that is never
 * known, so the literal is the path after an unknown base. Rust has no
 * exact or raw fold: the path must never reach CBMCallArg.value or
 * first_string_arg, where the route-registration reader would take it
 * (#3114). */
static const char *rust_helper_url(CBMExtractCtx *ctx, TSNode arg) {
    for (int i = 0; i < FOLD_MAX_DEPTH && ts_node_named_child_count(arg) > 0 &&
                    (strcmp(ts_node_type(arg), "try_expression") == 0 ||
                     strcmp(ts_node_type(arg), "parenthesized_expression") == 0);
         i++) {
        arg = ts_node_named_child(arg, 0);
    }
    TSNode args = is_url_builder_call(ctx, arg)
                      ? ts_node_child_by_field_name(arg, TS_FIELD("arguments"))
                      : (TSNode){0};
    if (ts_node_is_null(args) || ts_node_named_child_count(args) == 0) {
        return NULL;
    }
    bool is_format;
    TSNode lit = helper_literal(ctx, ts_node_named_child(args, 0), &is_format);
    const char *raw = ts_node_is_null(lit) ? NULL : fold_string_like(ctx, lit, 0, false);
    if (raw && is_format) {
        char flat[FOLD_BUF];
        raw = flatten_format(raw, flat, sizeof(flat)) ? cbm_arena_strdup(ctx->arena, flat) : NULL;
    }
    return raw ? path_after_unknown_base(ctx, raw) : NULL;
}

const char *cbm_url_fold_call_url(CBMExtractCtx *ctx, TSNode arg) {
    if (ctx->language == CBM_LANG_RUST && !ts_node_is_null(arg)) {
        return rust_helper_url(ctx, arg);
    }
    if (!cbm_url_fold_lang(ctx->language) || ts_node_is_null(arg) || !is_foldable_arg(arg)) {
        return NULL;
    }
    const char *s = fold_node(ctx, arg, 0, true);
    if (!s) {
        return NULL;
    }
    if (!at_placeholder(s)) {
        return has_literal_text(s) ? s : NULL;
    }
    /* Unresolvable base: keep the literal path after it, never the base. */
    return path_after_unknown_base(ctx, s + PAIR_LEN);
}

const char *cbm_url_fold_exact(CBMExtractCtx *ctx, TSNode arg) {
    if (!cbm_url_fold_lang(ctx->language) || ts_node_is_null(arg) || !is_foldable_arg(arg)) {
        return NULL;
    }
    const char *s = fold_node(ctx, arg, 0, false);
    return (s && !at_placeholder(s) && has_literal_text(s)) ? s : NULL;
}

const char *cbm_url_fold_call_raw(CBMExtractCtx *ctx, TSNode arg) {
    if (!cbm_url_fold_lang(ctx->language) || ts_node_is_null(arg) || !is_foldable_arg(arg)) {
        return NULL;
    }
    const char *s = fold_node(ctx, arg, 0, false);
    return s && has_literal_text(s) ? s : NULL;
}
