/*
 * helm_template.c -- Helm values and the rendering of a chart template's
 * image references (see helm_template.h).
 *
 * Values: YAML -> JSON as Helm does it (sigs.k8s.io/yaml over go-yaml v2):
 * YAML 1.1 booleans (yes/no/on/off), `~` / null / empty a null, every number
 * a float64 -- a float prints like Go's %v (1.10 -> 1.1, 20231010 ->
 * 2.023101e+07), the way Helm renders it. --set values keep int64.
 *
 * Rendering: Go text/template over the vendored gotmpl grammar -- text with
 * {{- / -}} trimming; if / else if / else, with, range (lists, maps by sorted
 * key, else, break / continue), variables (:= / =, `$` the template's data),
 * define / template / block / include / tpl, pipelines (the piped value is
 * the last argument); and the Sprig functions image expressions use. A missing
 * map key renders "" (Helm drops "<no value>"); a field of nil is an error.
 * An error, a function not modelled, .Release.Name / .Capabilities / lookup,
 * a define not found -- each is UNKNOWN, which spreads to what reads it; a
 * condition or range that is UNKNOWN runs no branch. Nesting of include /
 * template stops where Helm's does (1000), an error there too.
 */
#include "pipeline/helm_template.h"

#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/json_writer.h" /* cbm_json_writer_allocator */
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "cbm.h"
#include "lang_specs.h"

#include <ctype.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { HELM_MAX_NESTING = 1000 };    /* Helm engine's recursionMaxNums */
enum { HELM_MAX_SET_INDEX = 65536 }; /* Helm strvals' MaxIndex for `--set a[n]` */

/* A render doing more than this -- steps (an action, an evaluation, a range
 * iteration), bytes written to any sink, or memory it takes (the arena's
 * growth: captures, list copies, strings) -- is one Helm never finishes
 * either (a range over a huge int, defines that call each other in a fan):
 * it stops, and every probe of the template is UNKNOWN. Counted, never timed;
 * far above any real chart (the 20 infra corpora and the helm oracle charts
 * never reach it). */
enum { HELM_MAX_STEPS = 10000000 };
static const size_t HELM_MAX_OUTPUT = (size_t)CBM_SZ_64 * CBM_SZ_1K * CBM_SZ_1K;  /* 64 MiB */
static const size_t HELM_MAX_MEMORY = (size_t)CBM_SZ_256 * CBM_SZ_1K * CBM_SZ_1K; /* 256 MiB */

/* ── Values: YAML -> JSON ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    size_t len;
    yyjson_mut_val *val;
    long decodes; /* the nodes its value took to decode, its aliases expanded */
} hy_anchor_t;

typedef struct {
    yyjson_mut_doc *doc;
    const char *src;
    hy_anchor_t *anchors;
    int nanchors;
    int cap;
    bool unknown;
    long decodes;       /* nodes decoded, alias expansions included (go-yaml decodeCount) */
    long alias_decodes; /* of those, the ones an alias expanded (aliasCount) */
} hy_t;

/* go-yaml v2 (decode.go): a document whose decodes come mostly from alias
 * expansion -- a billion laughs -- is an error, so Helm reads no values. The
 * share allowed falls from 99% below 400,000 decodes to 10% above 4,000,000. */
enum {
    HY_ALIAS_MIN_ALIASES = 100,
    HY_ALIAS_MIN_DECODES = 1000,
    HY_ALIAS_RANGE_LOW = 400000,
    HY_ALIAS_RANGE_HIGH = 4000000
};
static const double HY_ALIAS_RATIO_SMALL = 0.99;
static const double HY_ALIAS_RATIO_LARGE = 0.10;

static bool hy_excessive_aliasing(const hy_t *y) {
    if (y->alias_decodes <= HY_ALIAS_MIN_ALIASES || y->decodes <= HY_ALIAS_MIN_DECODES) {
        return false;
    }
    double allowed = HY_ALIAS_RATIO_SMALL;
    if (y->decodes >= HY_ALIAS_RANGE_HIGH) {
        allowed = HY_ALIAS_RATIO_LARGE;
    } else if (y->decodes > HY_ALIAS_RANGE_LOW) {
        allowed = HY_ALIAS_RATIO_SMALL - (HY_ALIAS_RATIO_SMALL - HY_ALIAS_RATIO_LARGE) *
                                             ((double)(y->decodes - HY_ALIAS_RANGE_LOW) /
                                              (double)(HY_ALIAS_RANGE_HIGH - HY_ALIAS_RANGE_LOW));
    }
    return (double)y->alias_decodes / (double)y->decodes > allowed;
}

static bool hy_type(TSNode n, const char *t) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), t) == 0;
}

static const char *hy_text(const hy_t *y, TSNode n, size_t *len) {
    uint32_t b = ts_node_start_byte(n);
    *len = ts_node_end_byte(n) - b;
    return y->src + b;
}

static void hy_anchor(hy_t *y, const char *name, size_t len, yyjson_mut_val *v, long decodes) {
    if (y->nanchors == y->cap) {
        int cap = y->cap ? y->cap * PAIR_LEN : CBM_SZ_16;
        hy_anchor_t *g = cbm_realloc(CBM_MEM_CLASS_OTHER, y->anchors, (size_t)cap * sizeof(*g));
        if (!g) {
            y->unknown = true;
            return;
        }
        y->anchors = g;
        y->cap = cap;
    }
    y->anchors[y->nanchors++] = (hy_anchor_t){name, len, v, decodes};
}

/* An alias: its anchor's value decoded again (a copy), as go-yaml counts it. */
static yyjson_mut_val *hy_alias(hy_t *y, const char *name, size_t len) {
    for (int i = y->nanchors - SKIP_ONE; i >= 0; i--) {
        if (y->anchors[i].len == len && memcmp(y->anchors[i].name, name, len) == 0) {
            y->decodes += y->anchors[i].decodes;
            y->alias_decodes += y->anchors[i].decodes;
            if (hy_excessive_aliasing(y)) {
                y->unknown = true;
                return NULL;
            }
            return yyjson_mut_val_mut_copy(y->doc, y->anchors[i].val);
        }
    }
    y->unknown = true;
    return NULL;
}

static bool hy_in(const char *s, size_t n, const char *const *set) {
    for (; *set; set++) {
        if (strlen(*set) == n && memcmp(*set, s, n) == 0) {
            return true;
        }
    }
    return false;
}

/* A float of go-yaml v2's form: [-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)? */
static bool hy_float_form(const char *s, size_t n) {
    size_t i = 0;
    if (i < n && (s[i] == '-' || s[i] == '+')) {
        i++;
    }
    size_t digits = 0;
    while (i < n && isdigit((unsigned char)s[i])) {
        i++;
        digits++;
    }
    if (i < n && s[i] == '.') {
        i++;
        size_t frac = 0;
        while (i < n && isdigit((unsigned char)s[i])) {
            i++;
            frac++;
        }
        if (digits == 0 && frac == 0) {
            return false;
        }
    } else if (digits == 0) {
        return false;
    }
    if (i < n && (s[i] == 'e' || s[i] == 'E')) {
        i++;
        if (i < n && (s[i] == '-' || s[i] == '+')) {
            i++;
        }
        size_t ed = 0;
        while (i < n && isdigit((unsigned char)s[i])) {
            i++;
            ed++;
        }
        if (ed == 0) {
            return false;
        }
    }
    return i == n;
}

/* An integer the way go-yaml v2 reads one: strconv.ParseInt(s, 0, 64) after
 * dropping `_` -- decimal, 0x hex, 0b binary, a leading 0 octal. */
static bool hy_int_form(const char *s, size_t n, int64_t *out) {
    char buf[CBM_SZ_64];
    size_t k = 0;
    for (size_t i = 0; i < n && k + SKIP_ONE < sizeof(buf); i++) {
        if (s[i] != '_') {
            buf[k++] = s[i];
        }
    }
    if (k == 0 || k + SKIP_ONE >= sizeof(buf)) {
        return false;
    }
    buf[k] = '\0';
    const char *p = buf;
    bool neg = false;
    if (*p == '-' || *p == '+') {
        neg = *p == '-';
        p++;
    }
    int base = 10;
    if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
        base = 16;
        p += PAIR_LEN;
    } else if (p[0] == '0' && (p[1] == 'b' || p[1] == 'B')) {
        base = 2;
        p += PAIR_LEN;
    } else if (p[0] == '0' && p[1]) {
        base = 8;
        p++;
    }
    if (!*p) {
        return false;
    }
    char *end = NULL;
    long long v = strtoll(p, &end, base);
    if (!end || *end) {
        return false;
    }
    *out = neg ? -(int64_t)v : (int64_t)v;
    return true;
}

static yyjson_mut_val *hy_plain(hy_t *y, const char *s, size_t n) {
    static const char *const NULLS[] = {"", "~", "null", "Null", "NULL", NULL};
    static const char *const TRUES[] = {"y",    "Y",    "yes", "Yes", "YES", "true",
                                        "True", "TRUE", "on",  "On",  "ON",  NULL};
    static const char *const FALSES[] = {"n",     "N",     "no",  "No",  "NO",  "false",
                                         "False", "FALSE", "off", "Off", "OFF", NULL};
    static const char *const SPECIAL[] = {".inf",  ".Inf",  ".INF",  "+.inf", "+.Inf",
                                          "+.INF", "-.inf", "-.Inf", "-.INF", ".nan",
                                          ".NaN",  ".NAN",  NULL};
    if (hy_in(s, n, NULLS)) {
        return yyjson_mut_null(y->doc);
    }
    if (hy_in(s, n, TRUES)) {
        return yyjson_mut_true(y->doc);
    }
    if (hy_in(s, n, FALSES)) {
        return yyjson_mut_false(y->doc);
    }
    if (hy_in(s, n, SPECIAL)) {
        y->unknown = true; /* JSON holds no inf / nan: Helm fails here */
        return NULL;
    }
    char c = n ? s[0] : '\0';
    if (isdigit((unsigned char)c) || c == '-' || c == '+' || c == '.') {
        int64_t iv = 0;
        if (hy_int_form(s, n, &iv)) {
            return yyjson_mut_real(y->doc, (double)iv);
        }
        if (hy_float_form(s, n)) {
            char buf[CBM_SZ_64];
            snprintf(buf, sizeof(buf), "%.*s", (int)n, s);
            return yyjson_mut_real(y->doc, strtod(buf, NULL));
        }
    }
    return yyjson_mut_strncpy(y->doc, s, n);
}

static int hy_hex(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    c = (char)tolower((unsigned char)c);
    return c >= 'a' && c <= 'f' ? c - 'a' + 10 : -SKIP_ONE;
}

static size_t hy_utf8(unsigned long cp, char *out) {
    if (cp < 0x80) {
        out[0] = (char)cp;
        return SKIP_ONE;
    }
    if (cp < 0x800) {
        out[0] = (char)(0xC0 | (cp >> 6));
        out[1] = (char)(0x80 | (cp & 0x3F));
        return PAIR_LEN;
    }
    if (cp < 0x10000) {
        out[0] = (char)(0xE0 | (cp >> 12));
        out[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[2] = (char)(0x80 | (cp & 0x3F));
        return 3;
    }
    out[0] = (char)(0xF0 | (cp >> 18));
    out[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    out[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
    out[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

/* Fold a quoted scalar's line breaks: a break and the blanks around it
 * become one space, an empty line a newline. */
static size_t hy_fold(char *buf, size_t n) {
    size_t k = 0;
    for (size_t i = 0; i < n;) {
        if (buf[i] == '\n') {
            while (k > 0 && (buf[k - SKIP_ONE] == ' ' || buf[k - SKIP_ONE] == '\t')) {
                k--;
            }
            size_t breaks = 0;
            while (i < n && (buf[i] == '\n' || buf[i] == ' ' || buf[i] == '\t' || buf[i] == '\r')) {
                breaks += buf[i] == '\n';
                i++;
            }
            if (breaks > SKIP_ONE) {
                for (size_t b = SKIP_ONE; b < breaks; b++) {
                    buf[k++] = '\n';
                }
            } else {
                buf[k++] = ' ';
            }
            continue;
        }
        buf[k++] = buf[i++];
    }
    return k;
}

static yyjson_mut_val *hy_double(hy_t *y, const char *s, size_t n) {
    if (n < PAIR_LEN) {
        y->unknown = true;
        return NULL;
    }
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, n * 4 + SKIP_ONE);
    if (!buf) {
        y->unknown = true;
        return NULL;
    }
    size_t k = 0;
    for (size_t i = SKIP_ONE; i + SKIP_ONE < n; i++) {
        char c = s[i];
        if (c != '\\') {
            buf[k++] = c;
            continue;
        }
        if (++i + SKIP_ONE >= n + SKIP_ONE) {
            break;
        }
        c = s[i];
        static const char ESC_IN[] = "0abtnvfre \"/\\N_LP\t";
        static const char ESC_OUT[] = "\0\a\b\t\n\v\f\r\x1b \"/\\";
        const char *e = strchr(ESC_IN, c);
        if (c == '\n') {
            while (i + SKIP_ONE < n && (s[i + SKIP_ONE] == ' ' || s[i + SKIP_ONE] == '\t')) {
                i++;
            }
        } else if (e && (size_t)(e - ESC_IN) < sizeof(ESC_OUT) - SKIP_ONE) {
            buf[k++] = ESC_OUT[e - ESC_IN];
        } else if (c == 'x' || c == 'u' || c == 'U') {
            int digits = c == 'x' ? PAIR_LEN : c == 'u' ? 4 : 8;
            uint32_t cp = 0;
            for (int d = 0; d < digits; d++) {
                int h = i + SKIP_ONE < n ? hy_hex(s[i + SKIP_ONE]) : -SKIP_ONE;
                if (h < 0) {
                    y->unknown = true;
                    cbm_free(CBM_MEM_CLASS_OTHER, buf);
                    return NULL;
                }
                cp = cp * 16 + (uint32_t)h;
                i++;
            }
            k += hy_utf8(cp, buf + k);
        } else {
            y->unknown = true;
            cbm_free(CBM_MEM_CLASS_OTHER, buf);
            return NULL;
        }
    }
    k = hy_fold(buf, k);
    yyjson_mut_val *v = yyjson_mut_strncpy(y->doc, buf, k);
    cbm_free(CBM_MEM_CLASS_OTHER, buf);
    return v;
}

static yyjson_mut_val *hy_single(hy_t *y, const char *s, size_t n) {
    if (n < PAIR_LEN) {
        y->unknown = true;
        return NULL;
    }
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!buf) {
        y->unknown = true;
        return NULL;
    }
    size_t k = 0;
    for (size_t i = SKIP_ONE; i + SKIP_ONE < n; i++) {
        buf[k++] = s[i];
        if (s[i] == '\'' && i + PAIR_LEN < n && s[i + SKIP_ONE] == '\'') {
            i++;
        }
    }
    k = hy_fold(buf, k);
    yyjson_mut_val *v = yyjson_mut_strncpy(y->doc, buf, k);
    cbm_free(CBM_MEM_CLASS_OTHER, buf);
    return v;
}

/* A block scalar (| or >, chomping - / +, an explicit indentation digit). */
static yyjson_mut_val *hy_block(hy_t *y, const char *s, size_t n) {
    size_t i = 0;
    bool folded = n > 0 && s[0] == '>';
    char chomp = 'c';
    int explicit_indent = 0;
    for (i = SKIP_ONE; i < n && s[i] != '\n' && s[i] != '#'; i++) {
        if (s[i] == '-' || s[i] == '+') {
            chomp = s[i];
        } else if (isdigit((unsigned char)s[i])) {
            explicit_indent = s[i] - '0';
        }
    }
    while (i < n && s[i] != '\n') {
        i++;
    }
    i += i < n ? SKIP_ONE : 0;
    /* the content's indentation: the explicit one, else its first non-empty line's */
    size_t indent = 0;
    if (explicit_indent > 0) {
        /* relative to the parent's: the line indentation of the scalar's own start */
        const char *ls = s;
        while (ls > y->src && ls[-1] != '\n') {
            ls--;
        }
        size_t parent = 0;
        while (ls + parent < s && ls[parent] == ' ') {
            parent++;
        }
        indent = parent + (size_t)explicit_indent;
    } else {
        for (size_t j = i; j < n;) {
            size_t sp = 0;
            while (j + sp < n && s[j + sp] == ' ') {
                sp++;
            }
            if (j + sp < n && s[j + sp] != '\n') {
                indent = sp;
                break;
            }
            j += sp + SKIP_ONE;
        }
    }
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, n + PAIR_LEN);
    if (!buf) {
        y->unknown = true;
        return NULL;
    }
    size_t k = 0;
    bool prev_more = false; /* the previous line was more indented (folded keeps its break) */
    bool first = true;
    while (i < n) {
        size_t sp = 0;
        while (i + sp < n && s[i + sp] == ' ' && sp < indent) {
            sp++;
        }
        size_t e = i + sp;
        while (e < n && s[e] != '\n') {
            e++;
        }
        bool empty = e == i + sp;
        bool more = !empty && (s[i + sp] == ' ' || s[i + sp] == '\t');
        if (folded && !first && k > 0 && buf[k - SKIP_ONE] == '\n' && !empty && !more &&
            !prev_more) {
            /* a break between two plain lines folds into a space ... */
            size_t back = k - SKIP_ONE;
            bool only = back == 0 || buf[back - SKIP_ONE] != '\n';
            if (only) {
                buf[back] = ' ';
            } else {
                /* ... one of several breaks is dropped */
                k = back;
            }
        }
        memcpy(buf + k, s + i + sp, e - (i + sp));
        k += e - (i + sp);
        buf[k++] = '\n';
        prev_more = more;
        first = empty && first;
        i = e + SKIP_ONE;
    }
    if (chomp != '+') {
        while (k > 0 && buf[k - SKIP_ONE] == '\n') {
            k--;
        }
        if (chomp == 'c' && k > 0) {
            buf[k++] = '\n';
        }
    }
    yyjson_mut_val *v = yyjson_mut_strncpy(y->doc, buf, k);
    cbm_free(CBM_MEM_CLASS_OTHER, buf);
    return v;
}

static yyjson_mut_val *hy_node(hy_t *y, TSNode n);

/* A mapping key as JSON: its scalar's text (sigs.k8s.io/yaml stringifies). */
static yyjson_mut_val *hy_key(hy_t *y, TSNode k) {
    yyjson_mut_val *v = ts_node_is_null(k) ? yyjson_mut_null(y->doc) : hy_node(y, k);
    if (!v) {
        return NULL;
    }
    if (yyjson_mut_is_str(v)) {
        return v;
    }
    if (yyjson_mut_is_null(v)) {
        return yyjson_mut_strcpy(y->doc, "null");
    }
    if (yyjson_mut_is_bool(v)) {
        return yyjson_mut_strcpy(y->doc, yyjson_mut_get_bool(v) ? "true" : "false");
    }
    if (yyjson_mut_is_real(v)) {
        /* go-yaml v2 keeps an integer key an int: its decimal digits */
        double d = yyjson_mut_get_real(v);
        char buf[CBM_SZ_64];
        if (d == floor(d) && fabs(d) < 1e18) {
            snprintf(buf, sizeof(buf), "%lld", (long long)d);
        } else {
            snprintf(buf, sizeof(buf), "%.17g", d);
        }
        return yyjson_mut_strcpy(y->doc, buf);
    }
    y->unknown = true; /* a mapping / sequence as a key */
    return NULL;
}

/* YAML merge key: `<<: *a` / `<<: [*a, *b]` -- entries not already there. */
static void hy_merge(yyjson_mut_val *obj, yyjson_mut_val *src) {
    if (yyjson_mut_is_arr(src)) {
        size_t idx, max;
        yyjson_mut_val *it;
        yyjson_mut_arr_foreach(src, idx, max, it) {
            hy_merge(obj, it);
        }
        return;
    }
    if (!yyjson_mut_is_obj(src)) {
        return;
    }
    size_t idx, max;
    yyjson_mut_val *k, *v;
    yyjson_mut_obj_foreach(src, idx, max, k, v) {
        if (!yyjson_mut_obj_getn(obj, yyjson_mut_get_str(k), yyjson_mut_get_len(k))) {
            yyjson_mut_obj_add(obj, k, v);
        }
    }
}

static yyjson_mut_val *hy_mapping(hy_t *y, TSNode n, const char *pair_type) {
    yyjson_mut_val *obj = yyjson_mut_obj(y->doc);
    yyjson_mut_val *merges = NULL;
    for (uint32_t i = 0; obj && i < ts_node_named_child_count(n); i++) {
        TSNode p = ts_node_named_child(n, i);
        if (hy_type(p, "comment")) {
            continue;
        }
        TSNode kn = p;
        TSNode vn = {0};
        if (hy_type(p, pair_type)) {
            kn = ts_node_child_by_field_name(p, "key", (uint32_t)strlen("key"));
            vn = ts_node_child_by_field_name(p, "value", (uint32_t)strlen("value"));
        } else if (!hy_type(p, "flow_node")) {
            y->unknown = true;
            return NULL;
        }
        size_t kl = 0;
        const char *kt = ts_node_is_null(kn) ? "" : hy_text(y, kn, &kl);
        yyjson_mut_val *val = ts_node_is_null(vn) ? yyjson_mut_null(y->doc) : hy_node(y, vn);
        if (!val || y->unknown) {
            y->unknown = true;
            return NULL;
        }
        if (kl == PAIR_LEN && memcmp(kt, "<<", PAIR_LEN) == 0) {
            merges = merges ? merges : yyjson_mut_arr(y->doc);
            yyjson_mut_arr_append(merges, val);
            continue;
        }
        yyjson_mut_val *key = hy_key(y, kn);
        if (!key) {
            y->unknown = true;
            return NULL;
        }
        (void)yyjson_mut_obj_put(obj, key, val); /* a duplicate key: the last one */
    }
    if (obj && merges) {
        hy_merge(obj, merges);
    }
    return obj;
}

static yyjson_mut_val *hy_sequence(hy_t *y, TSNode n) {
    yyjson_mut_val *arr = yyjson_mut_arr(y->doc);
    for (uint32_t i = 0; arr && i < ts_node_named_child_count(n); i++) {
        TSNode it = ts_node_named_child(n, i);
        if (hy_type(it, "comment")) {
            continue;
        }
        TSNode content = it;
        if (hy_type(it, "block_sequence_item")) {
            content = ts_node_named_child_count(it) > 0 ? ts_node_named_child(it, 0) : (TSNode){0};
            while (!ts_node_is_null(content) && hy_type(content, "comment")) {
                content = ts_node_next_named_sibling(content);
            }
        }
        yyjson_mut_val *v = ts_node_is_null(content)        ? yyjson_mut_null(y->doc)
                            : hy_type(content, "flow_pair") ? NULL
                                                            : hy_node(y, content);
        if (hy_type(content, "flow_pair")) {
            /* [a: b] -- a single-pair map */
            v = yyjson_mut_obj(y->doc);
            TSNode kn = ts_node_child_by_field_name(content, "key", (uint32_t)strlen("key"));
            TSNode vn = ts_node_child_by_field_name(content, "value", (uint32_t)strlen("value"));
            yyjson_mut_val *key = hy_key(y, kn);
            yyjson_mut_val *val = ts_node_is_null(vn) ? yyjson_mut_null(y->doc) : hy_node(y, vn);
            if (!key || !val) {
                y->unknown = true;
                return NULL;
            }
            yyjson_mut_obj_add(v, key, val);
        }
        if (!v || y->unknown) {
            y->unknown = true;
            return NULL;
        }
        yyjson_mut_arr_append(arr, v);
    }
    return arr;
}

static yyjson_mut_val *hy_node(hy_t *y, TSNode n) {
    if (ts_node_is_null(n) || ts_node_has_error(n) || y->unknown) {
        y->unknown = true;
        return NULL;
    }
    const char *t = ts_node_type(n);
    if (strcmp(t, "block_node") == 0 || strcmp(t, "flow_node") == 0) {
        TSNode content = {0};
        const char *anchor = NULL;
        size_t al = 0;
        bool str_tag = false;
        for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
            TSNode c = ts_node_named_child(n, i);
            if (hy_type(c, "anchor")) {
                TSNode nm = ts_node_named_child(c, 0);
                anchor = ts_node_is_null(nm) ? NULL : hy_text(y, nm, &al);
            } else if (hy_type(c, "tag")) {
                size_t tl = 0;
                const char *tt = hy_text(y, c, &tl);
                str_tag = tl == strlen("!!str") && memcmp(tt, "!!str", tl) == 0;
            } else if (!hy_type(c, "comment")) {
                content = c;
            }
        }
        yyjson_mut_val *v = NULL;
        long before = y->decodes;
        if (ts_node_is_null(content)) {
            y->decodes++;
            v = yyjson_mut_null(y->doc);
        } else if (str_tag && hy_type(content, "plain_scalar")) {
            size_t l = 0;
            const char *s = hy_text(y, content, &l);
            y->decodes++;
            v = yyjson_mut_strncpy(y->doc, s, l);
        } else {
            v = hy_node(y, content);
        }
        if (v && anchor) {
            hy_anchor(y, anchor, al, v, y->decodes - before);
        }
        return v;
    }
    y->decodes++; /* a mapping, sequence, scalar or alias node */
    size_t l = 0;
    const char *s = hy_text(y, n, &l);
    if (strcmp(t, "block_mapping") == 0) {
        return hy_mapping(y, n, "block_mapping_pair");
    }
    if (strcmp(t, "flow_mapping") == 0) {
        return hy_mapping(y, n, "flow_pair");
    }
    if (strcmp(t, "block_sequence") == 0 || strcmp(t, "flow_sequence") == 0) {
        return hy_sequence(y, n);
    }
    if (strcmp(t, "plain_scalar") == 0) {
        return hy_plain(y, s, l);
    }
    if (strcmp(t, "double_quote_scalar") == 0) {
        return hy_double(y, s, l);
    }
    if (strcmp(t, "single_quote_scalar") == 0) {
        return hy_single(y, s, l);
    }
    if (strcmp(t, "block_scalar") == 0) {
        return hy_block(y, s, l);
    }
    if (strcmp(t, "alias") == 0) {
        TSNode nm = ts_node_named_child(n, 0);
        size_t nl = 0;
        const char *name = ts_node_is_null(nm) ? NULL : hy_text(y, nm, &nl);
        return name ? hy_alias(y, name, nl) : NULL;
    }
    y->unknown = true;
    return NULL;
}

static void hy_done(hy_t *y) {
    cbm_free(CBM_MEM_CLASS_OTHER, y->anchors);
    y->anchors = NULL;
}

yyjson_mut_val *cbm_helm_values_node(yyjson_mut_doc *doc, const char *src, TSNode node) {
    hy_t y = {.doc = doc, .src = src};
    yyjson_mut_val *v = hy_node(&y, node);
    hy_done(&y);
    return y.unknown ? NULL : v;
}

yyjson_mut_val *cbm_helm_values_parse(yyjson_mut_doc *doc, const char *src, size_t len) {
    TSParser *parser = ts_parser_new();
    if (!parser || !ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_YAML))) {
        if (parser) {
            ts_parser_delete(parser);
        }
        return NULL;
    }
    TSTree *tree = ts_parser_parse_string(parser, NULL, src, (uint32_t)len);
    yyjson_mut_val *out = NULL;
    if (tree) {
        TSNode root = ts_tree_root_node(tree);
        TSNode docn = {0};
        for (uint32_t i = 0; i < ts_node_named_child_count(root); i++) {
            if (hy_type(ts_node_named_child(root, i), "document")) {
                docn = ts_node_named_child(root, i);
                break;
            }
        }
        TSNode content = {0};
        for (uint32_t i = 0; !ts_node_is_null(docn) && i < ts_node_named_child_count(docn); i++) {
            TSNode c = ts_node_named_child(docn, i);
            if (!hy_type(c, "comment")) {
                content = c;
                break;
            }
        }
        if (ts_node_has_error(root) && (ts_node_is_null(docn) || ts_node_has_error(docn))) {
            out = NULL;
        } else if (ts_node_is_null(content)) {
            out = yyjson_mut_obj(doc);
        } else {
            out = cbm_helm_values_node(doc, src, content);
            if (out && yyjson_mut_is_null(out)) {
                out = yyjson_mut_obj(doc);
            }
        }
        ts_tree_delete(tree);
    }
    ts_parser_delete(parser);
    return out;
}

static yyjson_mut_val *hc_merge(yyjson_mut_doc *doc, yyjson_mut_val *dst, yyjson_mut_val *over) {
    if (!yyjson_mut_is_obj(dst) || !yyjson_mut_is_obj(over)) {
        return over ? yyjson_mut_val_mut_copy(doc, over) : dst;
    }
    size_t idx, max;
    yyjson_mut_val *k, *v;
    yyjson_mut_obj_foreach(over, idx, max, k, v) {
        const char *ks = yyjson_mut_get_str(k);
        size_t kl = yyjson_mut_get_len(k);
        if (yyjson_mut_is_null(v)) {
            (void)yyjson_mut_obj_remove_keyn(dst, ks, kl);
            continue;
        }
        yyjson_mut_val *cur = yyjson_mut_obj_getn(dst, ks, kl);
        yyjson_mut_val *merged = cur && yyjson_mut_is_obj(cur) && yyjson_mut_is_obj(v)
                                     ? hc_merge(doc, cur, v)
                                     : yyjson_mut_val_mut_copy(doc, v);
        if (merged != cur) {
            (void)yyjson_mut_obj_remove_keyn(dst, ks, kl);
            yyjson_mut_obj_add(dst, yyjson_mut_strncpy(doc, ks, kl), merged);
        }
    }
    return dst;
}

yyjson_mut_val *cbm_helm_values_coalesce(yyjson_mut_doc *doc, yyjson_mut_val *base,
                                         yyjson_mut_val *over) {
    yyjson_mut_val *out = base ? yyjson_mut_val_mut_copy(doc, base) : yyjson_mut_obj(doc);
    return over ? hc_merge(doc, out, over) : out;
}

/* Helm strvals' typedVal. */
static yyjson_mut_val *hs_typed(yyjson_mut_doc *doc, const char *v, bool as_string) {
    if (as_string) {
        return yyjson_mut_strcpy(doc, v);
    }
    if (strcasecmp(v, "true") == 0) {
        return yyjson_mut_true(doc);
    }
    if (strcasecmp(v, "false") == 0) {
        return yyjson_mut_false(doc);
    }
    if (strcasecmp(v, "null") == 0) {
        return yyjson_mut_null(doc);
    }
    if (strcmp(v, "0") == 0) {
        return yyjson_mut_sint(doc, 0);
    }
    if (v[0] && v[0] != '0') {
        char *end = NULL;
        long long iv = strtoll(v, &end, 10);
        if (end && !*end && (isdigit((unsigned char)v[0]) || v[0] == '-' || v[0] == '+')) {
            return yyjson_mut_sint(doc, iv);
        }
    }
    return yyjson_mut_strcpy(doc, v);
}

bool cbm_helm_values_set(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *name,
                         const char *value, bool as_string) {
    if (!yyjson_mut_is_obj(root) || !name || !name[0] || !value) {
        return false;
    }
    yyjson_mut_val *cur = root;
    const char *p = name;
    char key[CBM_SZ_512];
    while (*p) {
        size_t k = 0;
        while (*p && *p != '.' && *p != '[' && k + SKIP_ONE < sizeof(key)) {
            if (*p == '\\' && p[1]) {
                p++;
            }
            key[k++] = *p++;
        }
        key[k] = '\0';
        if (*p && *p != '.' && *p != '[') {
            return false; /* a key longer than the buffer: never split into two */
        }
        long index = -SKIP_ONE;
        if (*p == '[') {
            char *end = NULL;
            index = strtol(p + SKIP_ONE, &end, 10);
            /* strvals: a negative index or one past MaxIndex is an error */
            if (!end || *end != ']' || index < 0 || index > HELM_MAX_SET_INDEX) {
                return false;
            }
            p = end + SKIP_ONE;
        }
        bool last = !*p || (*p == '.' && !p[1]);
        if (*p == '.') {
            p++;
        }
        if (k == 0 || !yyjson_mut_is_obj(cur)) {
            return false;
        }
        yyjson_mut_val *child = yyjson_mut_obj_get(cur, key);
        if (index >= 0) {
            if (!yyjson_mut_is_arr(child)) {
                child = yyjson_mut_arr(doc);
                (void)yyjson_mut_obj_remove_key(cur, key);
                yyjson_mut_obj_add(cur, yyjson_mut_strcpy(doc, key), child);
            }
            while (yyjson_mut_arr_size(child) <= (size_t)index) {
                yyjson_mut_arr_append(child, yyjson_mut_null(doc));
            }
            if (last) {
                (void)yyjson_mut_arr_replace(child, (size_t)index, hs_typed(doc, value, as_string));
                return true;
            }
            yyjson_mut_val *el = yyjson_mut_arr_get(child, (size_t)index);
            if (!yyjson_mut_is_obj(el)) {
                el = yyjson_mut_obj(doc);
                (void)yyjson_mut_arr_replace(child, (size_t)index, el);
            }
            cur = el;
            continue;
        }
        if (last) {
            (void)yyjson_mut_obj_remove_key(cur, key);
            yyjson_mut_obj_add(cur, yyjson_mut_strcpy(doc, key), hs_typed(doc, value, as_string));
            return true;
        }
        if (!yyjson_mut_is_obj(child)) {
            child = yyjson_mut_obj(doc);
            (void)yyjson_mut_obj_remove_key(cur, key);
            yyjson_mut_obj_add(cur, yyjson_mut_strcpy(doc, key), child);
        }
        cur = child;
    }
    return false;
}

/* ── Defines ──────────────────────────────────────────────────────── */

typedef struct {
    char *rel;
    char *src;
    size_t len;
    TSTree *tree;
} hd_file_t;

typedef struct {
    const hd_file_t *file;
    TSNode node; /* the define_action / block_action */
    int depth;
} hd_def_t;

struct cbm_helm_defines {
    hd_file_t **files; /* each allocated once: defines point into them */
    int nfiles;
    int cap;
    CBMHashTable *names;  /* name -> hd_def_t* */
    CBMHashTable *by_rel; /* path -> hd_file_t* */
};

cbm_helm_defines_t *cbm_helm_defines_new(void) {
    cbm_helm_defines_t *d = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*d));
    if (d) {
        d->names = cbm_ht_create(0);
        d->by_rel = cbm_ht_create(0);
        if (!d->names || !d->by_rel) {
            cbm_helm_defines_free(d);
            return NULL;
        }
    }
    return d;
}

static int hd_depth(const char *rel) {
    int n = 0;
    for (const char *p = rel; *p; p++) {
        n += *p == '/';
    }
    return n;
}

/* A Go string literal's value ("..." with escapes, `...` raw); NULL: invalid. */
static char *hg_string(const char *s, size_t n, size_t *out_len);

static void hd_scan(cbm_helm_defines_t *d, hd_file_t *f, TSNode n, int depth) {
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        const char *t = ts_node_type(c);
        bool def = strcmp(t, "define_action") == 0;
        bool block = strcmp(t, "block_action") == 0;
        if (def || block) {
            TSNode nm = ts_node_child_by_field_name(c, "name", (uint32_t)strlen("name"));
            size_t nl = 0;
            char *name = ts_node_is_null(nm)
                             ? NULL
                             : hg_string(f->src + ts_node_start_byte(nm),
                                         ts_node_end_byte(nm) - ts_node_start_byte(nm), &nl);
            if (name) {
                hd_def_t *old = cbm_ht_get(d->names, name);
                bool wins = !old || depth < old->depth ||
                            (depth == old->depth && strcmp(f->rel, old->file->rel) < 0);
                hd_def_t *nd = wins ? cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*nd)) : NULL;
                if (nd) {
                    *nd = (hd_def_t){f, c, depth};
                    if (old) {
                        const char *key = cbm_ht_get_key(d->names, name);
                        cbm_ht_set(d->names, key, nd);
                        cbm_free(CBM_MEM_CLASS_OTHER, old);
                        cbm_free(CBM_MEM_CLASS_OTHER, name);
                    } else {
                        cbm_ht_set(d->names, name, nd);
                    }
                    name = NULL;
                }
                cbm_free(CBM_MEM_CLASS_OTHER, name);
            }
        }
        if (block || strcmp(t, "if_action") == 0 || strcmp(t, "with_action") == 0 ||
            strcmp(t, "range_action") == 0) {
            hd_scan(d, f, c, depth); /* a define is top-level; a block nests */
        }
    }
}

void cbm_helm_defines_add(cbm_helm_defines_t *d, const char *rel, const char *src, size_t len) {
    if (!d || !src) {
        return;
    }
    if (d->nfiles == d->cap) {
        int cap = d->cap ? d->cap * PAIR_LEN : CBM_SZ_16;
        hd_file_t **g = cbm_realloc(CBM_MEM_CLASS_OTHER, d->files, (size_t)cap * sizeof(*g));
        if (!g) {
            return;
        }
        d->files = g;
        d->cap = cap;
    }
    hd_file_t *f = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*f));
    if (!f) {
        return;
    }
    f->rel = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, rel);
    f->src = cbm_alloc(CBM_MEM_CLASS_OTHER, len + SKIP_ONE);
    if (!f->rel || !f->src) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->rel);
        cbm_free(CBM_MEM_CLASS_OTHER, f->src);
        cbm_free(CBM_MEM_CLASS_OTHER, f);
        return;
    }
    memcpy(f->src, src, len);
    f->src[len] = '\0';
    f->len = len;
    TSParser *parser = ts_parser_new();
    if (parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_GOTEMPLATE))) {
        f->tree = ts_parser_parse_string(parser, NULL, f->src, (uint32_t)len);
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    d->files[d->nfiles++] = f;
    cbm_ht_set(d->by_rel, f->rel, f);
    if (f->tree) {
        hd_scan(d, f, ts_tree_root_node(f->tree), hd_depth(rel));
    }
}

static void hd_free_def(const char *key, void *value, void *ud) {
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
    cbm_free(CBM_MEM_CLASS_OTHER, value);
}

void cbm_helm_defines_free(cbm_helm_defines_t *d) {
    if (!d) {
        return;
    }
    if (d->names) {
        cbm_ht_foreach(d->names, hd_free_def, NULL);
        cbm_ht_free(d->names);
    }
    if (d->by_rel) {
        cbm_ht_free(d->by_rel); /* its keys are the files' own */
    }
    for (int i = 0; i < d->nfiles; i++) {
        if (d->files[i]->tree) {
            ts_tree_delete(d->files[i]->tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, d->files[i]->rel);
        cbm_free(CBM_MEM_CLASS_OTHER, d->files[i]->src);
        cbm_free(CBM_MEM_CLASS_OTHER, d->files[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, d->files);
    cbm_free(CBM_MEM_CLASS_OTHER, d);
}

/* Template file `rel` of the set; NULL: none. */
static const hd_file_t *hd_file_find(const cbm_helm_defines_t *d, const char *rel) {
    return d && d->by_rel ? cbm_ht_get(d->by_rel, rel) : NULL;
}

static const hd_def_t *hd_find(const cbm_helm_defines_t *d, const char *name) {
    return d && d->names ? cbm_ht_get(d->names, name) : NULL;
}

/* ── Go literals and formatting ───────────────────────────────────── */

/* The value of Go string literal s ("..." or `...`) into out (capacity n);
 * its length, or (size_t)-1 when it is no valid literal. */
static size_t hg_unquote(const char *s, size_t n, char *out) {
    if (n >= PAIR_LEN && s[0] == '`' && s[n - SKIP_ONE] == '`') {
        memcpy(out, s + SKIP_ONE, n - PAIR_LEN);
        return n - PAIR_LEN;
    }
    if (n < PAIR_LEN || s[0] != '"' || s[n - SKIP_ONE] != '"') {
        return (size_t)-1;
    }
    size_t k = 0;
    for (size_t i = SKIP_ONE; i + SKIP_ONE < n; i++) {
        char c = s[i];
        if (c != '\\') {
            out[k++] = c;
            continue;
        }
        if (++i + SKIP_ONE >= n) {
            return (size_t)-1;
        }
        c = s[i];
        static const char IN[] = "abfnrtv\\\"'";
        static const char OUT[] = "\a\b\f\n\r\t\v\\\"'";
        const char *e = strchr(IN, c);
        if (e && c) {
            out[k++] = OUT[e - IN];
        } else if (c == 'x' || c == 'u' || c == 'U') {
            int digits = c == 'x' ? PAIR_LEN : c == 'u' ? 4 : 8;
            uint32_t cp = 0;
            for (int d = 0; d < digits; d++) {
                int h = i + SKIP_ONE + SKIP_ONE < n ? hy_hex(s[i + SKIP_ONE]) : -SKIP_ONE;
                if (h < 0) {
                    return (size_t)-1;
                }
                cp = cp * 16 + (uint32_t)h;
                i++;
            }
            if (c == 'x') {
                out[k++] = (char)cp;
            } else {
                k += hy_utf8(cp, out + k);
            }
        } else if (c >= '0' && c <= '7') {
            uint32_t v = 0;
            for (int d = 0; d < 3 && i + SKIP_ONE < n && s[i] >= '0' && s[i] <= '7'; d++, i++) {
                v = v * 8 + (uint32_t)(s[i] - '0');
            }
            i--;
            out[k++] = (char)v;
        } else {
            return (size_t)-1;
        }
    }
    return k;
}

static char *hg_string(const char *s, size_t n, size_t *out_len) {
    char *out = cbm_alloc(CBM_MEM_CLASS_OTHER, n * 4 + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t k = hg_unquote(s, n, out);
    if (k == (size_t)-1) {
        cbm_free(CBM_MEM_CLASS_OTHER, out);
        return NULL;
    }
    out[k] = '\0';
    *out_len = k;
    return out;
}

/* float64 as Go's %v writes it (strconv 'g', shortest). */
static void hg_float(double f, char *out, size_t sz) {
    if (f == 0) {
        snprintf(out, sz, signbit(f) ? "-0" : "0");
        return;
    }
    char e[CBM_SZ_64];
    int prec = 0;
    for (prec = 0; prec < 17; prec++) {
        snprintf(e, sizeof(e), "%.*e", prec, f);
        if (strtod(e, NULL) == f) {
            break;
        }
    }
    /* e: [-]d[.ddd]e(+|-)XX */
    bool neg = e[0] == '-';
    const char *p = e + (neg ? SKIP_ONE : 0);
    char digs[CBM_SZ_32];
    int nd = 0;
    for (; *p && *p != 'e'; p++) {
        if (isdigit((unsigned char)*p) && nd + SKIP_ONE < (int)sizeof(digs)) {
            digs[nd++] = *p;
        }
    }
    while (nd > SKIP_ONE && digs[nd - SKIP_ONE] == '0') {
        nd--;
    }
    digs[nd] = '\0';
    int exp = *p == 'e' ? atoi(p + SKIP_ONE) : 0;
    int dp = exp + SKIP_ONE;
    size_t k = 0;
    if (neg) {
        out[k++] = '-';
    }
    if (exp < -4 || exp >= 6) {
        out[k++] = digs[0];
        if (nd > SKIP_ONE) {
            out[k++] = '.';
            for (int i = SKIP_ONE; i < nd && k + 8 < sz; i++) {
                out[k++] = digs[i];
            }
        }
        snprintf(out + k, sz - k, "e%c%02d", exp < 0 ? '-' : '+', exp < 0 ? -exp : exp);
        return;
    }
    if (dp <= 0) {
        out[k++] = '0';
        out[k++] = '.';
        for (int i = 0; i < -dp && k + 4 < sz; i++) {
            out[k++] = '0';
        }
        for (int i = 0; i < nd && k + 4 < sz; i++) {
            out[k++] = digs[i];
        }
    } else {
        for (int i = 0; i < dp && k + 4 < sz; i++) {
            out[k++] = i < nd ? digs[i] : '0';
        }
        if (nd > dp) {
            out[k++] = '.';
            for (int i = dp; i < nd && k + 4 < sz; i++) {
                out[k++] = digs[i];
            }
        }
    }
    out[k] = '\0';
}

bool cbm_helm_scalar_text(yyjson_mut_val *v, char *out, size_t sz) {
    if (yyjson_mut_is_str(v)) {
        snprintf(out, sz, "%s", yyjson_mut_get_str(v));
    } else if (yyjson_mut_is_real(v)) {
        hg_float(yyjson_mut_get_real(v), out, sz);
    } else if (yyjson_mut_is_int(v)) {
        snprintf(out, sz, "%lld",
                 yyjson_mut_is_sint(v) ? (long long)yyjson_mut_get_sint(v)
                                       : (long long)yyjson_mut_get_uint(v));
    } else if (yyjson_mut_is_bool(v)) {
        snprintf(out, sz, "%s", yyjson_mut_get_bool(v) ? "true" : "false");
    } else {
        return false;
    }
    return true;
}

/* ── Runtime values ───────────────────────────────────────────────── */

typedef enum {
    HV_UNK = 0,
    HV_NIL,
    HV_BOOL,
    HV_INT,
    HV_FLOAT,
    HV_STR,
    HV_JSON,
    HV_MAP,
    HV_LIST
} hv_kind_t;

typedef struct hv hv_t;

typedef struct {
    const char **keys;
    hv_t *vals;
    int n;
    bool strict; /* a struct (.Chart, .Release): a field not held is UNKNOWN */
} hv_map_t;

typedef struct {
    hv_t *items;
    int n;
} hv_list_t;

struct hv {
    hv_kind_t k;
    bool b;
    int64_t i;
    double f;
    const char *s;
    size_t sn;
    yyjson_mut_val *j; /* HV_JSON: an object or array of the values */
    hv_map_t *m;
    hv_list_t *l;
    const char *path; /* the values path it holds; NULL: none */
};

static const hv_t HV_UNKNOWN = {.k = HV_UNK};

static hv_t hv_nil(void) {
    return (hv_t){.k = HV_NIL};
}

static hv_t hv_bool(bool b) {
    return (hv_t){.k = HV_BOOL, .b = b};
}

static hv_t hv_int(int64_t i) {
    return (hv_t){.k = HV_INT, .i = i};
}

static hv_t hv_float(double f) {
    return (hv_t){.k = HV_FLOAT, .f = f};
}

static hv_t hv_str(const char *s, size_t n) {
    return (hv_t){.k = HV_STR, .s = s, .sn = n};
}

static hv_t hv_json(yyjson_mut_val *v, const char *path) {
    hv_t out = {.path = path};
    if (!v || yyjson_mut_is_null(v)) {
        out.k = HV_NIL;
    } else if (yyjson_mut_is_bool(v)) {
        out.k = HV_BOOL;
        out.b = yyjson_mut_get_bool(v);
    } else if (yyjson_mut_is_int(v)) {
        out.k = HV_INT;
        out.i = yyjson_mut_is_sint(v) ? yyjson_mut_get_sint(v) : (int64_t)yyjson_mut_get_uint(v);
    } else if (yyjson_mut_is_real(v)) {
        out.k = HV_FLOAT;
        out.f = yyjson_mut_get_real(v);
    } else if (yyjson_mut_is_str(v)) {
        out.k = HV_STR;
        out.s = yyjson_mut_get_str(v);
        out.sn = yyjson_mut_get_len(v);
    } else if (yyjson_mut_is_obj(v) || yyjson_mut_is_arr(v)) {
        out.k = HV_JSON;
        out.j = v;
    }
    return out;
}

static bool hv_is_map(const hv_t *v) {
    return v->k == HV_MAP || (v->k == HV_JSON && yyjson_mut_is_obj(v->j));
}

static bool hv_is_list(const hv_t *v) {
    return v->k == HV_LIST || (v->k == HV_JSON && yyjson_mut_is_arr(v->j));
}

static int hv_len(const hv_t *v) {
    switch (v->k) {
    case HV_STR:
        return (int)v->sn;
    case HV_MAP:
        return v->m->n;
    case HV_LIST:
        return v->l->n;
    case HV_JSON:
        return (int)(yyjson_mut_is_obj(v->j) ? yyjson_mut_obj_size(v->j)
                                             : yyjson_mut_arr_size(v->j));
    default:
        return -SKIP_ONE;
    }
}

/* Go template truth (if / with / and / or / not). */
static bool hv_truth(const hv_t *v) {
    switch (v->k) {
    case HV_BOOL:
        return v->b;
    case HV_INT:
        return v->i != 0;
    case HV_FLOAT:
        return v->f != 0;
    case HV_STR:
    case HV_MAP:
    case HV_LIST:
    case HV_JSON:
        return hv_len(v) > 0;
    default:
        return false;
    }
}

/* ── Renderer state ───────────────────────────────────────────────── */

typedef struct {
    const char *name;
    size_t nl;
    hv_t v;
} hvar_t;

typedef struct {
    char *buf;
    size_t n;
    size_t cap;
    bool unk;
} hsink_t;

typedef struct {
    const char *src; /* the text these nodes are of */
    hv_t dot;
    int var_base; /* the template's first variable (`$`) */
    hsink_t *out; /* NULL: the template's own output, which the probes read */
} hframe_t;

typedef struct {
    CBMArena *a;
    yyjson_mut_doc *scratch;
    const cbm_helm_defines_t *defs;
    hvar_t *vars;
    int nvars;
    int vcap;
    int nesting;
    bool brk;
    bool cont;
    long steps;
    size_t written;  /* bytes appended to any sink */
    size_t arena0;   /* the arena's total when the render began */
    bool stop;       /* over HELM_MAX_STEPS / _OUTPUT / _MEMORY: the render is UNKNOWN */
    const char *why; /* which budget stopped it */
    const char *rel; /* the template's path (logs) */
    /* maps a set / unset / merge changed in place (not modelled): what they
     * hold is UNKNOWN from then on -- by address ("%p" keys in the arena),
     * and for a merge of values also by values path (everything below it) */
    CBMHashTable *poison_maps;
    CBMHashTable *poison_paths;
    /* the values path of each values map changed (a set or a merge): a merge
     * above one of them reaches what it put there */
    const char **changes;
    int nchanges;
    int chcap;
    /* a change of maps not run exactly: every capture ending later is UNKNOWN */
    bool tainted;
    CBMHashTable *mutates;      /* define name -> it may change a map (he_define_may_mutate) */
    CBMHashTable *file_mutates; /* template file -> the same (he_file_may_mutate) */
    const char *base_path;      /* .Template.BasePath; NULL: not known */
    const char *chart_dir;      /* where the chart's template files are (meta.dir) */
    long busy_hits;             /* defines met again while being scanned */
    /* probes of the top-level template */
    const char *top_src;
    size_t top_len;
    cbm_helm_probe_t *probes;
    int nprobes;
    const uint32_t *lines; /* line start offsets */
    int nlines;
    int capture; /* the probe capturing; -1: none */
    hsink_t cap;
    const char **ckeys;
    int nck;
    int ckcap;
    CBMHashTable *ckset; /* ckeys as a set */
    TSParser *parser;
    TSTree **trees;
    int ntrees;
    int tcap;
} hst_t;

/* Is the render over its memory budget (the arena grown since it began)? */
static bool hst_over_memory(const hst_t *st) {
    return cbm_arena_total(st->a) - st->arena0 > HELM_MAX_MEMORY;
}

/* The render stops: budget `why` is spent. */
static void hst_trip(hst_t *st, const char *why) {
    if (!st->stop) {
        st->stop = true;
        st->why = why;
    }
}

/* One step of work; false once the render is over its budget. */
static bool hst_step(hst_t *st) {
    if (!st->stop && ++st->steps > HELM_MAX_STEPS) {
        hst_trip(st, "steps");
    } else if (!st->stop && hst_over_memory(st)) {
        hst_trip(st, "memory");
    }
    return !st->stop;
}

/* n more bytes of output; false (and the render stopped) past the budget. */
static bool hst_write(hst_t *st, size_t n) {
    if (!st->stop && n > HELM_MAX_OUTPUT - st->written) {
        hst_trip(st, "output");
    } else if (!st->stop && hst_over_memory(st)) {
        hst_trip(st, "memory");
    }
    if (st->stop) {
        return false;
    }
    st->written += n;
    return true;
}

/* A change of maps the render does not run exactly (see he_call): from here
 * on no capture is known. */
static void hst_taint(hst_t *st, const char *cause) {
    if (!st->tainted) {
        st->tainted = true;
        cbm_log_debug("helm.render.taint", "file", st->rel ? st->rel : "", "cause", cause);
    }
}

static void hs_append(hst_t *st, hsink_t *s, const char *p, size_t n) {
    if (s->unk || n == 0) {
        return;
    }
    if (!hst_write(st, n)) {
        s->unk = true;
        return;
    }
    if (s->n + n + SKIP_ONE > s->cap) {
        size_t cap = (s->cap ? s->cap : CBM_SZ_64) * PAIR_LEN;
        while (cap < s->n + n + SKIP_ONE) {
            cap *= PAIR_LEN;
        }
        char *g = cbm_arena_alloc(st->a, cap);
        if (!g) {
            s->unk = true;
            return;
        }
        if (s->n) {
            memcpy(g, s->buf, s->n);
        }
        s->buf = g;
        s->cap = cap;
    }
    memcpy(s->buf + s->n, p, n);
    s->n += n;
    s->buf[s->n] = '\0';
}

static const char *hs_str(hst_t *st, const hsink_t *s) {
    return s->n ? s->buf : cbm_arena_strdup(st->a, "");
}

static void hst_record(hst_t *st, const char *path) {
    if (st->capture < 0 || !path) {
        return;
    }
    if (!st->ckset) {
        st->ckset = cbm_ht_create(0);
    }
    if (!st->ckset) {
        st->cap.unk = true; /* a key read not kept: the capture's mark is not whole */
        return;
    }
    if (cbm_ht_get(st->ckset, path)) {
        return;
    }
    if (st->nck == st->ckcap) {
        int cap = st->ckcap ? st->ckcap * PAIR_LEN : CBM_SZ_16;
        const char **g = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*g));
        if (!g) {
            st->cap.unk = true;
            return;
        }
        if (st->nck) {
            memcpy(g, st->ckeys, (size_t)st->nck * sizeof(*g));
        }
        st->ckeys = g;
        st->ckcap = cap;
    }
    cbm_ht_set(st->ckset, path, (void *)(uintptr_t)SKIP_ONE);
    st->ckeys[st->nck++] = path;
}

/* A values path joined with key `k` (its own `.` escaped). */
static const char *hst_path(hst_t *st, const char *path, const char *k, size_t kl) {
    if (!path) {
        return NULL;
    }
    size_t dots = 0;
    for (size_t i = 0; i < kl; i++) {
        dots += k[i] == '.';
    }
    size_t pl = strlen(path);
    char *out = cbm_arena_alloc(st->a, pl + kl + dots + PAIR_LEN);
    if (!out) {
        return NULL;
    }
    size_t o = 0;
    if (pl) {
        memcpy(out, path, pl);
        o = pl;
        out[o++] = '.';
    }
    for (size_t i = 0; i < kl; i++) {
        if (k[i] == '.') {
            out[o++] = '\\';
        }
        out[o++] = k[i];
    }
    out[o] = '\0';
    return out;
}

static bool hst_var_push(hst_t *st, const char *name, size_t nl, hv_t v) {
    if (st->nvars == st->vcap) {
        int cap = st->vcap ? st->vcap * PAIR_LEN : CBM_SZ_32;
        hvar_t *g = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*g));
        if (!g) {
            return false;
        }
        if (st->nvars) {
            memcpy(g, st->vars, (size_t)st->nvars * sizeof(*g));
        }
        st->vars = g;
        st->vcap = cap;
    }
    st->vars[st->nvars++] = (hvar_t){name, nl, v};
    return true;
}

static hvar_t *hst_var(hst_t *st, const hframe_t *fr, const char *name, size_t nl) {
    for (int i = st->nvars - SKIP_ONE; i >= fr->var_base; i--) {
        if (st->vars[i].nl == nl && memcmp(st->vars[i].name, name, nl) == 0) {
            return &st->vars[i];
        }
    }
    return NULL;
}

/* Has a set / unset / merge changed map v (or, for a merge, a map holding
 * it)? Then what v holds is UNKNOWN. */
static bool hv_poisoned(const hst_t *st, const hv_t *v) {
    const void *map = v->k == HV_MAP    ? (const void *)v->m
                      : v->k == HV_JSON ? (const void *)v->j
                                        : NULL;
    if (!map) {
        return false;
    }
    char key[CBM_SZ_32];
    snprintf(key, sizeof(key), "%p", map);
    if (st->poison_maps && cbm_ht_get(st->poison_maps, key)) {
        return true;
    }
    if (!st->poison_paths || !v->path) {
        return false;
    }
    /* a merged values path covers what is below it: each prefix of v's path
     * that ends at a key ("", "a", "a.b", "a.b[0]" ...; an escaped `\.` is no
     * end) is looked up */
    size_t pl = strlen(v->path);
    char small[CBM_SZ_256];
    char *buf = pl < sizeof(small) ? small : cbm_alloc(CBM_MEM_CLASS_OTHER, pl + SKIP_ONE);
    bool hit = false;
    for (size_t i = 0; buf && i <= pl && !hit; i++) {
        bool end = i == pl || ((v->path[i] == '.' || v->path[i] == '[') &&
                               !(i > 0 && v->path[i - SKIP_ONE] == '\\'));
        if (!end) {
            continue;
        }
        memcpy(buf, v->path, i);
        buf[i] = '\0';
        hit = cbm_ht_get(st->poison_paths, buf) != NULL;
    }
    if (buf != small) {
        cbm_free(CBM_MEM_CLASS_OTHER, buf);
    }
    return hit || !buf;
}

/* Key `k` into set *set (made when NULL; the key copied into the arena). */
static bool hst_set_add(hst_t *st, CBMHashTable **set, const char *k) {
    if (!*set) {
        *set = cbm_ht_create(0);
    }
    if (!*set) {
        return false;
    }
    if (cbm_ht_get(*set, k)) {
        return true;
    }
    const char *dup = cbm_arena_strdup(st->a, k);
    if (!dup) {
        return false;
    }
    cbm_ht_set(*set, dup, (void *)(uintptr_t)SKIP_ONE);
    return true;
}

static bool hst_poison_at(hst_t *st, const hv_t *v, bool deep, int depth);

/* Map v changed in place; false when v is no map, when (`deep`, a merge)
 * a map within it cannot be poisoned whole, or with no room to say so. A
 * merge may change every map within it too -- a map the template made (dict)
 * holds others by reference, so those are poisoned as well. */
static bool hst_poison(hst_t *st, const hv_t *v, bool deep) {
    return hst_poison_at(st, v, deep, 0);
}

/* Has a values map at path p or below it been changed? */
static bool hst_changed_below(hst_t *st, const char *p) {
    size_t lp = strlen(p);
    for (int i = 0; i < st->nchanges; i++) {
        const char *c = st->changes[i];
        if (!hst_step(st) ||
            (strncmp(c, p, lp) == 0 && (c[lp] == '\0' || c[lp] == '.' || c[lp] == '['))) {
            return true;
        }
    }
    return false;
}

/* Values map at path p changed in place; false with no room to say so. */
static bool hst_record_change(hst_t *st, const char *p) {
    if (st->nchanges == st->chcap) {
        int cap = st->chcap ? st->chcap * PAIR_LEN : CBM_SZ_16;
        const char **g = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*g));
        if (!g) {
            return false;
        }
        if (st->nchanges) {
            memcpy(g, st->changes, (size_t)st->nchanges * sizeof(*g));
        }
        st->changes = g;
        st->chcap = cap;
    }
    st->changes[st->nchanges++] = p;
    return true;
}

static bool hst_poison_at(hst_t *st, const hv_t *v, bool deep, int depth) {
    if (depth > HELM_MAX_NESTING || !hst_step(st)) {
        return false;
    }
    const void *map = v->k == HV_MAP                               ? (const void *)v->m
                      : v->k == HV_JSON && yyjson_mut_is_obj(v->j) ? (const void *)v->j
                                                                   : NULL;
    if (!map) {
        return false;
    }
    /* a deep change reaches every map within: one not known, one a change
     * already made unknown, the values root (every path below it), a map not
     * of the values (whose maps no path covers), or a values map below which
     * a change was made (it may hold another's map now) cannot be poisoned
     * whole */
    if (deep && (hv_poisoned(st, v) || (v->path && !v->path[0]) || (v->k == HV_JSON && !v->path) ||
                 (v->path && hst_changed_below(st, v->path)))) {
        return false;
    }
    if (deep && v->k == HV_MAP) {
        for (int i = 0; i < v->m->n; i++) {
            const hv_t *x = &v->m->vals[i];
            if (x->k == HV_UNK || (hv_is_map(x) && !hst_poison_at(st, x, deep, depth + SKIP_ONE))) {
                return false;
            }
        }
    }
    char key[CBM_SZ_32];
    snprintf(key, sizeof(key), "%p", map);
    if (!hst_set_add(st, &st->poison_maps, key) || (v->path && !hst_record_change(st, v->path))) {
        return false;
    }
    return !(deep && v->path) || hst_set_add(st, &st->poison_paths, v->path);
}

static bool hn_is(TSNode n, const char *t) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), t) == 0;
}

static const char *hn_text(const hframe_t *fr, TSNode n, size_t *len) {
    uint32_t b = ts_node_start_byte(n);
    *len = ts_node_end_byte(n) - b;
    return fr->src + b;
}

static TSNode hn_field(TSNode n, const char *f) {
    return ts_node_child_by_field_name(n, f, (uint32_t)strlen(f));
}

/* The name of a variable node ("" for `$`). */
static const char *hn_var_name(const hframe_t *fr, TSNode var, size_t *nl) {
    TSNode nm = hn_field(var, "name");
    if (ts_node_is_null(nm)) {
        *nl = 0;
        return "";
    }
    return hn_text(fr, nm, nl);
}

/* ── Printing ─────────────────────────────────────────────────────── */

/* Go's %v of a basic value into the arena; NULL for one not modelled. */
static const char *hv_fmt(hst_t *st, const hv_t *v, size_t *len) {
    char buf[CBM_SZ_64];
    switch (v->k) {
    case HV_STR:
        *len = v->sn;
        return v->s;
    case HV_BOOL:
        *len = v->b ? 4 : 5;
        return v->b ? "true" : "false";
    case HV_INT:
        snprintf(buf, sizeof(buf), "%lld", (long long)v->i);
        break;
    case HV_FLOAT:
        hg_float(v->f, buf, sizeof(buf));
        break;
    case HV_NIL:
        *len = 5;
        return "<nil>";
    default:
        return NULL;
    }
    char *out = cbm_arena_strdup(st->a, buf);
    *len = out ? strlen(out) : 0;
    return out;
}

/* Sprig's strval. */
static bool hv_strval(hst_t *st, const hv_t *v, hv_t *out) {
    size_t n = 0;
    const char *s = hv_fmt(st, v, &n);
    if (!s) {
        return false;
    }
    *out = hv_str(s, n);
    return true;
}

/* ── Output and probes ────────────────────────────────────────────── */

/* A capture's scalar with its YAML quoting removed; NULL: none / invalid. */
static const char *hst_scalar(hst_t *st, const char *s, size_t n) {
    while (n && (*s == ' ' || *s == '\t')) {
        s++;
        n--;
    }
    while (n && (s[n - SKIP_ONE] == ' ' || s[n - SKIP_ONE] == '\t' || s[n - SKIP_ONE] == '\r')) {
        n--;
    }
    if (n == 0) {
        return NULL;
    }
    if (s[0] == '"' || s[0] == '\'') {
        /* the closing quote: \" escapes one in "..", '' one in '..' */
        size_t e = SKIP_ONE;
        for (; e < n; e++) {
            if (s[0] == '"' && s[e] == '\\') {
                e++;
            } else if (s[0] == '\'' && s[e] == '\'' && e + SKIP_ONE < n &&
                       s[e + SKIP_ONE] == '\'') {
                e++;
            } else if (s[e] == s[0]) {
                break;
            }
        }
        if (e >= n) {
            return NULL;
        }
        for (size_t r = e + SKIP_ONE; r < n; r++) {
            if (s[r] == '#') {
                break;
            }
            if (s[r] != ' ' && s[r] != '\t') {
                return NULL;
            }
        }
        yyjson_mut_val *v = cbm_helm_values_parse(st->scratch, s, e + SKIP_ONE);
        return v && yyjson_mut_is_str(v) ? cbm_arena_strdup(st->a, yyjson_mut_get_str(v)) : NULL;
    }
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '#' && i > 0 && (s[i - SKIP_ONE] == ' ' || s[i - SKIP_ONE] == '\t')) {
            n = i;
            break;
        }
        if (s[i] == ':' && (i + SKIP_ONE == n || s[i + SKIP_ONE] == ' ')) {
            return NULL; /* a mapping, no scalar */
        }
    }
    while (n && (s[n - SKIP_ONE] == ' ' || s[n - SKIP_ONE] == '\t')) {
        n--;
    }
    return n ? cbm_arena_strndup(st->a, s, n) : NULL;
}

/* strcmp order of string s against k + "." (lk = strlen(k)): 0 for each s
 * that starts with it. */
static int hst_cmp_below(const char *s, const char *k, size_t lk) {
    int r = strncmp(s, k, lk);
    if (r != 0) {
        return r;
    }
    unsigned char c = (unsigned char)s[lk];
    return c < '.' ? -SKIP_ONE : c > '.' ? SKIP_ONE : 0;
}

/* Does sorted (n keys, strcmp order) hold a key below k (k + "." ...)? */
static bool hst_has_below(const char *const *sorted, int n, const char *k) {
    size_t lk = strlen(k);
    int lo = 0;
    int hi = n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        if (hst_cmp_below(sorted[mid], k, lk) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo < n && hst_cmp_below(sorted[lo], k, lk) == 0;
}

static int hst_str_cmp(const void *x, const void *y) {
    return strcmp(*(const char *const *)x, *(const char *const *)y);
}

/* The leaves the capture read as a JSON array in the order read (NULL: none,
 * or *whole false: not built in full). A path that is the prefix of another
 * (at a `.`) is dropped. One sort and one allocation: k keys cost O(k log k)
 * steps and their bytes of memory. */
static const char *hst_capture_keys(hst_t *st, bool *whole) {
    *whole = true;
    int n = st->nck;
    if (n == 0) {
        return NULL;
    }
    const char **sorted = cbm_arena_alloc(st->a, (size_t)n * sizeof(*sorted));
    bool *keep = cbm_arena_alloc(st->a, (size_t)n * sizeof(*keep));
    if (!sorted || !keep) {
        *whole = false;
        return NULL;
    }
    memcpy(sorted, st->ckeys, (size_t)n * sizeof(*sorted));
    qsort(sorted, (size_t)n, sizeof(*sorted), hst_str_cmp);
    size_t total = PAIR_LEN; /* [ ] */
    int kept = 0;
    for (int i = 0; i < n; i++) {
        if (!hst_step(st)) {
            *whole = false;
            return NULL;
        }
        const char *k = st->ckeys[i];
        keep[i] = k[0] && !hst_has_below(sorted, n, k);
        if (!keep[i]) {
            continue;
        }
        total += PAIR_LEN + (kept ? SKIP_ONE : 0); /* "" and , */
        for (const char *c = k; *c; c++) {
            total += *c == '"' || *c == '\\' ? PAIR_LEN : SKIP_ONE;
        }
        kept++;
    }
    if (kept == 0) {
        return NULL;
    }
    char *json = cbm_arena_alloc(st->a, total + SKIP_ONE);
    if (!json) {
        *whole = false;
        return NULL;
    }
    size_t o = 0;
    json[o++] = '[';
    for (int i = 0; i < n; i++) {
        if (!keep[i]) {
            continue;
        }
        if (o > SKIP_ONE) {
            json[o++] = ',';
        }
        json[o++] = '"';
        for (const char *c = st->ckeys[i]; *c; c++) {
            if (*c == '"' || *c == '\\') {
                json[o++] = '\\';
            }
            json[o++] = *c;
        }
        json[o++] = '"';
    }
    json[o++] = ']';
    json[o] = '\0';
    return json;
}

/* The capture (if any) ends: its image and the keys it read join its probe. */
static void hst_capture_end(hst_t *st) {
    if (st->capture < 0) {
        return;
    }
    cbm_helm_probe_t *p = &st->probes[st->capture];
    if (p->n == p->cap) {
        /* the captures of a probe grow by doubling: n captures cost O(n) */
        int cap = p->cap ? p->cap * PAIR_LEN : CBM_SZ_4;
        const char **gi = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*gi));
        const char **gk = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*gk));
        if (gi && gk) {
            if (p->n) {
                memcpy(gi, p->image, (size_t)p->n * sizeof(*gi));
                memcpy(gk, p->keys, (size_t)p->n * sizeof(*gk));
            }
            p->image = gi;
            p->keys = gk;
            p->cap = cap;
        }
    }
    if (p->n < p->cap) {
        bool whole = true;
        const char *keys = hst_capture_keys(st, &whole);
        p->image[p->n] = st->cap.unk || st->tainted || !whole
                             ? NULL /* a mark not written in full: no image, never one unmarked */
                             : hst_scalar(st, st->cap.buf ? st->cap.buf : "", st->cap.n);
        p->keys[p->n] = whole ? keys : NULL;
        p->n++;
    }
    /* the keys this capture recorded, one by one: a clear walks the set's
     * whole capacity, which one huge capture leaves behind for every later
     * one */
    for (int i = 0; st->ckset && i < st->nck; i++) {
        (void)cbm_ht_delete(st->ckset, st->ckeys[i]);
    }
    st->capture = -SKIP_ONE;
    st->cap = (hsink_t){0};
    st->nck = 0;
}

/* Output of the top-level template: into the capture until its newline. */
static void hst_out(hst_t *st, hframe_t *fr, const char *p, size_t n) {
    if (fr->out) {
        hs_append(st, fr->out, p, n);
        return;
    }
    if (st->capture < 0) {
        return;
    }
    const char *nl = memchr(p, '\n', n);
    hs_append(st, &st->cap, p, nl ? (size_t)(nl - p) : n);
    if (nl) {
        hst_capture_end(st);
    }
}

static void hst_out_unknown(hst_t *st, hframe_t *fr) {
    if (fr->out) {
        fr->out->unk = true;
    } else if (st->capture >= 0) {
        st->cap.unk = true;
    }
}

/* Output of a value as text/template prints it (nil: "", as Helm). */
static void hst_out_value(hst_t *st, hframe_t *fr, const hv_t *v) {
    if (v->k == HV_NIL) {
        return;
    }
    size_t n = 0;
    const char *s = hv_fmt(st, v, &n);
    if (!s) {
        hst_out_unknown(st, fr);
        return;
    }
    hst_out(st, fr, s, n);
}

static bool hn_ws(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

/* A text node, trimmed by an adjacent `-}}` / `{{-`; at the top level each
 * probe whose `image:` key it holds starts its capture after the key. */
static void hst_text(hst_t *st, hframe_t *fr, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    /* the markers are lexical -- `{{- ` / ` -}}` beside the text, whichever
     * action (of whatever nesting) they open or close */
    if (b >= 4 && strncmp(fr->src + b - 3, "-}}", 3) == 0 && hn_ws(fr->src[b - 4])) {
        while (b < e && hn_ws(fr->src[b])) {
            b++;
        }
    }
    if (strncmp(fr->src + e, "{{-", 3) == 0 && hn_ws(fr->src[e + 3])) {
        while (e > b && hn_ws(fr->src[e - SKIP_ONE])) {
            e--;
        }
    }
    if (fr->out || fr->src != st->top_src) {
        hst_out(st, fr, fr->src + b, e - b);
        return;
    }
    uint32_t at = b;
    for (;;) {
        /* the next probe key in [at, e) */
        uint32_t key = e;
        int which = -SKIP_ONE;
        for (int i = 0; i < st->nprobes; i++) {
            int line = st->probes[i].line;
            if (line < SKIP_ONE || line > st->nlines) {
                continue;
            }
            uint32_t k = st->lines[line - SKIP_ONE];
            while (k < st->top_len && (st->top_src[k] == ' ' || st->top_src[k] == '\t')) {
                k++;
            }
            if (k < st->top_len && st->top_src[k] == '-' && k + SKIP_ONE < st->top_len &&
                st->top_src[k + SKIP_ONE] == ' ') {
                k++;
                while (k < st->top_len && st->top_src[k] == ' ') {
                    k++;
                }
            }
            if (k >= at && k < key && k + strlen("image:") <= e &&
                memcmp(st->top_src + k, "image:", strlen("image:")) == 0) {
                key = k;
                which = i;
            }
        }
        if (which < 0) {
            hst_out(st, fr, fr->src + at, e - at);
            return;
        }
        uint32_t after = key + (uint32_t)strlen("image:");
        hst_out(st, fr, fr->src + at, after - at);
        hst_capture_end(st);
        st->capture = which;
        st->cap = (hsink_t){0};
        st->nck = 0;
        at = after;
    }
}

/* ── Evaluation ───────────────────────────────────────────────────── */

static hv_t he_eval(hst_t *st, hframe_t *fr, TSNode n);
static void he_items(hst_t *st, hframe_t *fr, TSNode parent, const char *field);
static hv_t he_render_define(hst_t *st, const char *name, hv_t arg, hsink_t *into);

/* Field `name` of value v. */
static hv_t he_field(hst_t *st, const hv_t *v, const char *name, size_t nl) {
    if (hv_poisoned(st, v)) {
        return HV_UNKNOWN;
    }
    if (v->k == HV_MAP) {
        for (int i = 0; i < v->m->n; i++) {
            if (strlen(v->m->keys[i]) == nl && memcmp(v->m->keys[i], name, nl) == 0) {
                hst_record(st, v->m->vals[i].path);
                return v->m->vals[i];
            }
        }
        return v->m->strict ? HV_UNKNOWN : hv_nil();
    }
    if (v->k == HV_JSON && yyjson_mut_is_obj(v->j)) {
        const char *path = hst_path(st, v->path, name, nl);
        yyjson_mut_val *c = yyjson_mut_obj_getn(v->j, name, nl);
        hst_record(st, path);
        return hv_json(c, path);
    }
    return HV_UNKNOWN; /* a field of nil, of a scalar, of a list: an error */
}

static hv_t he_index1(hst_t *st, const hv_t *v, const hv_t *k) {
    if (hv_poisoned(st, v)) {
        return HV_UNKNOWN;
    }
    if (hv_is_map(v)) {
        if (k->k != HV_STR) {
            return HV_UNKNOWN;
        }
        return he_field(st, v, k->s, k->sn);
    }
    if (hv_is_list(v)) {
        if (k->k != HV_INT) {
            return HV_UNKNOWN;
        }
        if (v->k == HV_LIST) {
            return k->i >= 0 && k->i < v->l->n ? v->l->items[k->i] : HV_UNKNOWN;
        }
        yyjson_mut_val *c = k->i >= 0 ? yyjson_mut_arr_get(v->j, (size_t)k->i) : NULL;
        if (!c) {
            return HV_UNKNOWN;
        }
        const char *path =
            v->path ? cbm_arena_sprintf(st->a, "%s[%lld]", v->path, (long long)k->i) : NULL;
        hst_record(st, path);
        return hv_json(c, path);
    }
    return v->k == HV_NIL ? hv_nil() : HV_UNKNOWN;
}

/* Sprig empty. */
static bool hv_empty(const hv_t *v) {
    return !hv_truth(v);
}

static bool hv_eq(const hv_t *a, const hv_t *b, bool *ok) {
    *ok = true;
    if (a->k == HV_STR && b->k == HV_STR) {
        return a->sn == b->sn && memcmp(a->s, b->s, a->sn) == 0;
    }
    if (a->k == HV_BOOL && b->k == HV_BOOL) {
        return a->b == b->b;
    }
    if (a->k == HV_INT && b->k == HV_INT) {
        return a->i == b->i;
    }
    if (a->k == HV_FLOAT && b->k == HV_FLOAT) {
        return a->f == b->f;
    }
    *ok = false; /* incompatible kinds: an error in Go */
    return false;
}

static hv_t hv_mklist(hst_t *st, const hv_t *items, int n) {
    hv_list_t *l = cbm_arena_alloc(st->a, sizeof(*l));
    hv_t *v = n ? cbm_arena_alloc(st->a, (size_t)n * sizeof(*v)) : NULL;
    if (!l || (n && !v)) {
        return HV_UNKNOWN;
    }
    if (n) {
        memcpy(v, items, (size_t)n * sizeof(*v));
    }
    *l = (hv_list_t){v, n};
    return (hv_t){.k = HV_LIST, .l = l};
}

/* The items of a list value into an arena array; -1 when it is none. */
static int hv_items(hst_t *st, const hv_t *v, hv_t **out) {
    if (v->k == HV_LIST) {
        *out = v->l->items;
        return v->l->n;
    }
    if (v->k == HV_JSON && yyjson_mut_is_arr(v->j)) {
        int n = (int)yyjson_mut_arr_size(v->j);
        hv_t *items = n ? cbm_arena_alloc(st->a, (size_t)n * sizeof(*items)) : NULL;
        if (n && !items) {
            return -SKIP_ONE;
        }
        size_t idx, max;
        yyjson_mut_val *it;
        yyjson_mut_arr_foreach(v->j, idx, max, it) {
            const char *path = v->path ? cbm_arena_sprintf(st->a, "%s[%zu]", v->path, idx) : NULL;
            items[idx] = hv_json(it, path);
        }
        *out = items;
        return n;
    }
    if (v->k == HV_NIL) {
        *out = NULL;
        return 0;
    }
    return -SKIP_ONE;
}

static int he_key_cmp(const void *x, const void *y) {
    return strcmp(*(const char *const *)x, *(const char *const *)y);
}

/* A map's keys, sorted (Go's order for range), into an arena array. */
static int hv_keys(hst_t *st, const hv_t *v, const char ***out) {
    int n = hv_len(v);
    const char **keys = n > 0 ? cbm_arena_alloc(st->a, (size_t)n * sizeof(*keys)) : NULL;
    if (n > 0 && !keys) {
        return -SKIP_ONE;
    }
    if (v->k == HV_MAP) {
        for (int i = 0; i < n; i++) {
            keys[i] = v->m->keys[i];
        }
    } else {
        size_t idx, max;
        yyjson_mut_val *k, *val;
        yyjson_mut_obj_foreach(v->j, idx, max, k, val) {
            (void)val;
            keys[idx] = yyjson_mut_get_str(k);
        }
    }
    if (n > SKIP_ONE) {
        qsort(keys, (size_t)n, sizeof(*keys), he_key_cmp);
    }
    *out = keys;
    return n;
}

static bool hv_to_int(const hv_t *v, int64_t *out) {
    switch (v->k) {
    case HV_INT:
        *out = v->i;
        return true;
    case HV_FLOAT:
        *out = (int64_t)v->f;
        return true;
    case HV_BOOL:
        *out = v->b ? SKIP_ONE : 0;
        return true;
    case HV_NIL:
        *out = 0;
        return true;
    case HV_STR: {
        char buf[CBM_SZ_64];
        if (v->sn == 0 || v->sn >= sizeof(buf)) {
            *out = 0;
            return v->sn == 0;
        }
        memcpy(buf, v->s, v->sn);
        buf[v->sn] = '\0';
        char *end = NULL;
        long long i = strtoll(buf, &end, 0);
        if (end && !*end) {
            *out = i;
            return true;
        }
        double d = strtod(buf, &end);
        *out = end && !*end ? (int64_t)d : 0;
        return true;
    }
    default:
        return false;
    }
}

/* Go's %q of s. */
static const char *hg_quote(hst_t *st, const char *s, size_t n, char q) {
    char *out = cbm_arena_alloc(st->a, n * 4 + 3);
    if (!out) {
        return NULL;
    }
    size_t k = 0;
    out[k++] = q;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (q == '"' && (c == '"' || c == '\\')) {
            out[k++] = '\\';
            out[k++] = (char)c;
        } else if (q == '"' && c < 0x20) {
            static const char HEX[] = "0123456789abcdef";
            const char *named = c == '\n' ? "\\n" : c == '\t' ? "\\t" : c == '\r' ? "\\r" : NULL;
            if (named) {
                out[k++] = named[0];
                out[k++] = named[1];
            } else {
                out[k++] = '\\';
                out[k++] = 'x';
                out[k++] = HEX[c >> 4];
                out[k++] = HEX[c & 0xF];
            }
        } else {
            out[k++] = (char)c;
        }
    }
    out[k++] = q;
    out[k] = '\0';
    return out;
}

/* fmt.Sprintf for the verbs %s %v %d %q %% without flags; NULL otherwise. */
static const char *he_printf(hst_t *st, const hv_t *args, int n) {
    if (n < SKIP_ONE || args[0].k != HV_STR) {
        return NULL;
    }
    hsink_t out = {0};
    int ai = SKIP_ONE;
    const char *f = args[0].s;
    size_t fl = args[0].sn;
    for (size_t i = 0; i < fl; i++) {
        if (f[i] != '%') {
            hs_append(st, &out, f + i, SKIP_ONE);
            continue;
        }
        if (++i >= fl) {
            return NULL;
        }
        char verb = f[i];
        if (verb == '%') {
            hs_append(st, &out, "%", SKIP_ONE);
            continue;
        }
        if (ai >= n) {
            return NULL;
        }
        const hv_t *a = &args[ai++];
        size_t sl = 0;
        const char *s = NULL;
        if (verb == 's' && a->k == HV_STR) {
            s = a->s;
            sl = a->sn;
        } else if (verb == 'v') {
            s = hv_fmt(st, a, &sl);
        } else if (verb == 'd' && a->k == HV_INT) {
            s = hv_fmt(st, a, &sl);
        } else if (verb == 'q' && a->k == HV_STR) {
            s = hg_quote(st, a->s, a->sn, '"');
            sl = s ? strlen(s) : 0;
        }
        if (!s) {
            return NULL; /* %!s(float64=...) and the like: no image */
        }
        hs_append(st, &out, s, sl);
    }
    if (ai != n || out.unk) {
        return NULL;
    }
    return hs_str(st, &out);
}

static hv_t he_strfn(hst_t *st, const char *fn, const hv_t *a, int n) {
    /* Sprig string functions take the subject last. */
    hv_t s;
    if (n < SKIP_ONE || !hv_strval(st, &a[n - SKIP_ONE], &s)) {
        return HV_UNKNOWN;
    }
    if (strcmp(fn, "trim") == 0 && n == SKIP_ONE) {
        size_t b = 0;
        size_t e = s.sn;
        while (b < e && hn_ws(s.s[b])) {
            b++;
        }
        while (e > b && hn_ws(s.s[e - SKIP_ONE])) {
            e--;
        }
        return hv_str(s.s + b, e - b);
    }
    if ((strcmp(fn, "lower") == 0 || strcmp(fn, "upper") == 0) && n == SKIP_ONE) {
        char *o = cbm_arena_strndup(st->a, s.s, s.sn);
        for (size_t i = 0; o && i < s.sn; i++) {
            o[i] =
                (char)(fn[0] == 'l' ? tolower((unsigned char)o[i]) : toupper((unsigned char)o[i]));
        }
        return o ? hv_str(o, s.sn) : HV_UNKNOWN;
    }
    if (n != PAIR_LEN && strcmp(fn, "replace") != 0) {
        return HV_UNKNOWN;
    }
    hv_t p;
    if (!hv_strval(st, &a[0], &p)) {
        return HV_UNKNOWN;
    }
    if (strcmp(fn, "trimPrefix") == 0) {
        bool has = p.sn <= s.sn && memcmp(s.s, p.s, p.sn) == 0;
        return has ? hv_str(s.s + p.sn, s.sn - p.sn) : s;
    }
    if (strcmp(fn, "trimSuffix") == 0) {
        bool has = p.sn <= s.sn && memcmp(s.s + s.sn - p.sn, p.s, p.sn) == 0;
        return has ? hv_str(s.s, s.sn - p.sn) : s;
    }
    if (strcmp(fn, "trimAll") == 0) {
        size_t b = 0;
        size_t e = s.sn;
        while (b < e && memchr(p.s, s.s[b], p.sn)) {
            b++;
        }
        while (e > b && memchr(p.s, s.s[e - SKIP_ONE], p.sn)) {
            e--;
        }
        return hv_str(s.s + b, e - b);
    }
    if (strcmp(fn, "contains") == 0 || strcmp(fn, "hasPrefix") == 0 ||
        strcmp(fn, "hasSuffix") == 0) {
        if (p.sn > s.sn) {
            return hv_bool(false);
        }
        if (fn[0] == 'c') {
            char *hs = cbm_arena_strndup(st->a, s.s, s.sn);
            char *ns = cbm_arena_strndup(st->a, p.s, p.sn);
            return hs && ns ? hv_bool(strstr(hs, ns) != NULL) : HV_UNKNOWN;
        }
        const char *at = fn[3] == 'P' ? s.s : s.s + s.sn - p.sn;
        return hv_bool(memcmp(at, p.s, p.sn) == 0);
    }
    if (strcmp(fn, "trunc") == 0) {
        int64_t c = 0;
        if (!hv_to_int(&a[0], &c)) {
            return HV_UNKNOWN;
        }
        if (c >= 0) {
            return hv_str(s.s, (size_t)c < s.sn ? (size_t)c : s.sn);
        }
        size_t keep = (size_t)(-c) < s.sn ? (size_t)(-c) : s.sn;
        return hv_str(s.s + s.sn - keep, keep);
    }
    if (strcmp(fn, "replace") == 0 && n == 3) {
        hv_t to;
        if (!hv_strval(st, &a[SKIP_ONE], &to) || p.sn == 0) {
            return HV_UNKNOWN;
        }
        hsink_t out = {0};
        for (size_t i = 0; i < s.sn;) {
            if (i + p.sn <= s.sn && memcmp(s.s + i, p.s, p.sn) == 0) {
                hs_append(st, &out, to.s, to.sn);
                i += p.sn;
            } else {
                hs_append(st, &out, s.s + i, SKIP_ONE);
                i++;
            }
        }
        return out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    return HV_UNKNOWN;
}

/* Sprig functions that change their argument in place: set / unset / merge
 * a map, sortAlpha a []string (splitList, keys, regexSplit give one; a list
 * is never poisoned, so it always counts as a change not run). Every other
 * list function copies its argument (push, append, prepend, reverse, uniq,
 * without, compact, rest, initial, concat, chunk) or reads it (first, last,
 * has, slice -- which shares it but changes nothing). */
static bool he_mutator(const char *s, size_t n) {
    static const char *const MUTATORS[] = {
        "set",       "unset", "merge", "mergeOverwrite", "mustMerge", "mustMergeOverwrite",
        "sortAlpha", NULL};
    return hy_in(s, n, MUTATORS);
}

static bool he_define_may_mutate(hst_t *st, const char *name, int depth);

/* The define a call / action names with a string literal into the arena;
 * NULL (and *computed) when the name is no literal. */
static const char *he_called_define(hst_t *st, const char *src, TSNode c, bool *computed) {
    TSNode nm = {0};
    *computed = false;
    if (hn_is(c, "function_call")) {
        TSNode fn = hn_field(c, "function");
        TSNode args = hn_field(c, "arguments");
        uint32_t fb = ts_node_is_null(fn) ? 0 : ts_node_start_byte(fn);
        size_t fl = ts_node_is_null(fn) ? 0 : ts_node_end_byte(fn) - fb;
        if (fl != strlen("include") || strncmp(src + fb, "include", fl) != 0) {
            return NULL;
        }
        nm = ts_node_is_null(args) ? (TSNode){0} : ts_node_named_child(args, 0);
    } else {
        nm = hn_field(c, "name");
    }
    if (!hn_is(nm, "interpreted_string_literal") && !hn_is(nm, "raw_string_literal")) {
        *computed = true;
        return NULL;
    }
    uint32_t b = ts_node_start_byte(nm);
    size_t l = ts_node_end_byte(nm) - b;
    char *name = cbm_arena_alloc(st->a, l * 4 + SKIP_ONE);
    size_t k = name ? hg_unquote(src + b, l, name) : (size_t)-1;
    if (k == (size_t)-1) {
        *computed = true;
        return NULL;
    }
    name[k] = '\0';
    return name;
}

/* The text of call node c's function (a function_call's, or a bare
 * identifier's); NULL when it has none. */
static const char *he_fn_text(const char *src, TSNode c, size_t *n) {
    TSNode fn = hn_is(c, "identifier") ? c : hn_field(c, "function");
    if (ts_node_is_null(fn)) {
        return NULL;
    }
    uint32_t b = ts_node_start_byte(fn);
    *n = ts_node_end_byte(fn) - b;
    return src + b;
}

static bool he_is_fn(const char *s, size_t n, const char *name) {
    return n == strlen(name) && memcmp(s, name, n) == 0;
}

/* May running node n (of text src) change a map in place? It may when it
 * holds a set / unset / merge (whatever the target), a tpl (its text is not
 * looked into), or an include / template of a name not known or of one that
 * may (he_define_may_mutate). A variable's name ($set) is no call. */
static bool he_may_mutate(hst_t *st, const char *src, TSNode n, int depth) {
    if (depth > HELM_MAX_NESTING || ts_node_has_error(n)) {
        return true; /* a form the grammar rejects (Go may not) may hide a change */
    }
    TSTreeCursor cur = ts_tree_cursor_new(n);
    bool found = false;
    bool more = true;
    while (more && !found && hst_step(st)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        const char *t = ts_node_type(c);
        size_t fl = 0;
        const char *fs = NULL;
        bool call = strcmp(t, "function_call") == 0 ||
                    (strcmp(t, "identifier") == 0 && hn_is(ts_node_parent(c), "chained_pipeline"));
        if (call && (fs = he_fn_text(src, c, &fl)) != NULL &&
            (he_mutator(fs, fl) || he_is_fn(fs, fl, "tpl"))) {
            found = true;
        } else if (strcmp(t, "function_call") == 0 || strcmp(t, "template_action") == 0 ||
                   strcmp(t, "block_action") == 0) {
            bool computed = false;
            const char *name = he_called_define(st, src, c, &computed);
            found = computed || (name && he_define_may_mutate(st, name, depth + SKIP_ONE));
        }
        if (found || (strcmp(t, "variable") != 0 && ts_tree_cursor_goto_first_child(&cur))) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
    return found || st->stop;
}

/* Memo states of he_define_may_mutate / he_file_may_mutate. */
enum { HM_NO = 1, HM_YES = 2, HM_BUSY = 3, HM_STALE = 4 };

/* The chart's template file an include names -- Helm names each one
 * `<.Template.BasePath>/<file>` -- or NULL: none of this chart's. */
static const hd_file_t *he_template_file(hst_t *st, const char *name) {
    if (!st->base_path || !st->chart_dir) {
        return NULL;
    }
    size_t bl = strlen(st->base_path);
    if (strncmp(name, st->base_path, bl) != 0 || name[bl] != '/') {
        return NULL;
    }
    const char *rel = cbm_arena_sprintf(st->a, "%s%stemplates/%s", st->chart_dir,
                                        st->chart_dir[0] ? "/" : "", name + bl + SKIP_ONE);
    return rel ? hd_file_find(st->defs, rel) : NULL;
}

/* May template file f, included by its name and so run whole, change a map
 * in place (he_may_mutate over all of it)? Memoized; a file reached again
 * while it is being scanned -- one including itself -- may. A no that leaned
 * on a define still being scanned is not kept. */
static bool he_file_may_mutate(hst_t *st, const hd_file_t *f, int depth) {
    if (!f->tree) {
        return true;
    }
    if (!st->file_mutates) {
        st->file_mutates = cbm_ht_create(0);
    }
    if (!st->file_mutates) {
        return true;
    }
    uintptr_t memo = (uintptr_t)cbm_ht_get(st->file_mutates, f->rel);
    if (memo) {
        return memo != HM_NO;
    }
    cbm_ht_set(st->file_mutates, f->rel, (void *)(uintptr_t)HM_BUSY);
    long hits = st->busy_hits;
    bool m = he_may_mutate(st, f->src, ts_tree_root_node(f->tree), depth);
    if (m || st->busy_hits == hits) {
        cbm_ht_set(st->file_mutates, f->rel, (void *)(uintptr_t)(m ? HM_YES : HM_NO));
    } else {
        (void)cbm_ht_delete(st->file_mutates, f->rel);
    }
    return m;
}

/* Memoized per define. A define reached again while it is being scanned
 * counts as no for now; a no that leaned on such a one is not kept (STALE:
 * scanned again when next asked), so defines including each other never fix
 * a wrong no -- a yes is final. */
static bool he_define_may_mutate(hst_t *st, const char *name, int depth) {
    if (!st->mutates) {
        st->mutates = cbm_ht_create(0);
    }
    uintptr_t memo = st->mutates ? (uintptr_t)cbm_ht_get(st->mutates, name) : 0;
    if (memo == HM_YES || memo == HM_NO) {
        return memo == HM_YES;
    }
    const hd_def_t *d = hd_find(st->defs, name);
    if (!d) {
        /* a template file of the chart by its name (the checksum idiom) is
         * looked into; a name not the chart's (a library's) may. Kept by
         * name once final, so an include in a loop asks once */
        const hd_file_t *f = he_template_file(st, name);
        bool m = f ? he_file_may_mutate(st, f, depth) : true;
        bool final =
            m || (st->file_mutates && (uintptr_t)cbm_ht_get(st->file_mutates, f->rel) == HM_NO);
        const char *key = final && st->mutates ? cbm_arena_strdup(st->a, name) : NULL;
        if (key) {
            cbm_ht_set(st->mutates, key, (void *)(uintptr_t)(m ? HM_YES : HM_NO));
        }
        return m;
    }
    if (memo == HM_BUSY) {
        st->busy_hits++;
        return false;
    }
    const char *key = memo ? cbm_ht_get_key(st->mutates, name) : cbm_arena_strdup(st->a, name);
    if (st->mutates && key) {
        cbm_ht_set(st->mutates, key, (void *)(uintptr_t)HM_BUSY);
    }
    long hits = st->busy_hits;
    bool m = he_may_mutate(st, d->file->src, d->node, depth);
    bool leaned = st->busy_hits > hits; /* on a define still being scanned */
    if (st->mutates && key) {
        uintptr_t res = m ? HM_YES : leaned ? HM_STALE : HM_NO;
        cbm_ht_set(st->mutates, key, (void *)res);
    }
    return m;
}

static hv_t he_apply_fn(hst_t *st, hframe_t *fr, const char *fn, hv_t *a, int n) {
    for (int i = 0; i < n; i++) {
        if (a[i].k == HV_UNK || hv_poisoned(st, &a[i])) {
            /* an include not run (an argument UNKNOWN or poisoned) that may
             * change maps, or a tpl not run: what it would change is missed */
            bool runs_code = strcmp(fn, "include") == 0 || strcmp(fn, "tpl") == 0;
            const char *dn = strcmp(fn, "include") == 0 && a[0].k == HV_STR
                                 ? cbm_arena_strndup(st->a, a[0].s, a[0].sn)
                                 : NULL;
            if (runs_code && (!dn || he_define_may_mutate(st, dn, st->nesting))) {
                hst_taint(st, fn);
            }
            return HV_UNKNOWN;
        }
    }
    if (strcmp(fn, "include") == 0 && n == PAIR_LEN && a[0].k == HV_STR) {
        hsink_t out = {0};
        char *name = cbm_arena_strndup(st->a, a[0].s, a[0].sn);
        hv_t r = name ? he_render_define(st, name, a[1], &out) : HV_UNKNOWN;
        return r.k == HV_UNK || out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    if (strcmp(fn, "tpl") == 0 && n == PAIR_LEN && a[0].k == HV_STR) {
        if (!st->parser) {
            hst_taint(st, "tpl");
            return HV_UNKNOWN;
        }
        char *src = cbm_arena_strndup(st->a, a[0].s, a[0].sn);
        TSTree *t = src ? ts_parser_parse_string(st->parser, NULL, src, (uint32_t)a[0].sn) : NULL;
        if (!t) {
            return HV_UNKNOWN;
        }
        if (st->ntrees == st->tcap) {
            int cap = st->tcap ? st->tcap * PAIR_LEN : CBM_SZ_16;
            TSTree **g = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*g));
            if (!g) {
                ts_tree_delete(t);
                return HV_UNKNOWN;
            }
            if (st->ntrees) {
                memcpy(g, st->trees, (size_t)st->ntrees * sizeof(*g));
            }
            st->trees = g;
            st->tcap = cap;
        }
        st->trees[st->ntrees++] = t;
        TSNode root = ts_tree_root_node(t);
        if (ts_node_has_error(root) || ++st->nesting > HELM_MAX_NESTING) {
            st->nesting -= ts_node_has_error(root) ? 0 : SKIP_ONE;
            hst_taint(st, "tpl");
            return HV_UNKNOWN;
        }
        hsink_t out = {0};
        int mark = st->nvars;
        hframe_t sub = {.src = src, .dot = a[1], .var_base = st->nvars, .out = &out};
        hst_var_push(st, "", 0, a[1]);
        he_items(st, &sub, root, NULL);
        st->nvars = mark;
        st->nesting--;
        return out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    if (strcmp(fn, "default") == 0 && n >= SKIP_ONE) {
        return n == SKIP_ONE || hv_empty(&a[SKIP_ONE]) ? a[0] : a[SKIP_ONE];
    }
    if (strcmp(fn, "coalesce") == 0) {
        for (int i = 0; i < n; i++) {
            if (!hv_empty(&a[i])) {
                return a[i];
            }
        }
        return hv_nil();
    }
    if (strcmp(fn, "ternary") == 0 && n == 3) {
        return a[2].k == HV_BOOL ? (a[2].b ? a[0] : a[SKIP_ONE]) : HV_UNKNOWN;
    }
    if (strcmp(fn, "empty") == 0 && n == SKIP_ONE) {
        return hv_bool(hv_empty(&a[0]));
    }
    if (strcmp(fn, "not") == 0 && n == SKIP_ONE) {
        return hv_bool(!hv_truth(&a[0]));
    }
    if ((strcmp(fn, "eq") == 0 && n >= PAIR_LEN) || (strcmp(fn, "ne") == 0 && n == PAIR_LEN)) {
        bool any = false;
        for (int i = SKIP_ONE; i < n; i++) {
            bool ok = false;
            any = any || hv_eq(&a[0], &a[i], &ok);
            if (!ok) {
                return HV_UNKNOWN;
            }
        }
        return hv_bool(fn[0] == 'e' ? any : !any);
    }
    if (strcmp(fn, "required") == 0 && n == PAIR_LEN) {
        bool missing = a[1].k == HV_NIL || (a[1].k == HV_STR && a[1].sn == 0);
        return missing ? HV_UNKNOWN : a[1];
    }
    if (strcmp(fn, "printf") == 0) {
        const char *s = he_printf(st, a, n);
        return s ? hv_str(s, strlen(s)) : HV_UNKNOWN;
    }
    if (strcmp(fn, "print") == 0 || strcmp(fn, "cat") == 0) {
        hsink_t out = {0};
        bool prev_str = true;
        int written = 0;
        for (int i = 0; i < n; i++) {
            if (fn[0] == 'c' && a[i].k == HV_NIL) {
                continue;
            }
            size_t sl = 0;
            const char *s = hv_fmt(st, &a[i], &sl);
            if (!s) {
                return HV_UNKNOWN;
            }
            bool is_str = a[i].k == HV_STR;
            /* fmt.Sprint spaces two operands neither of which is a string; cat always */
            if (written && (fn[0] == 'c' || (!is_str && !prev_str))) {
                hs_append(st, &out, " ", SKIP_ONE);
            }
            hs_append(st, &out, s, sl);
            prev_str = is_str;
            written++;
        }
        return out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    if (strcmp(fn, "quote") == 0 || strcmp(fn, "squote") == 0) {
        hsink_t out = {0};
        int written = 0;
        for (int i = 0; i < n; i++) {
            hv_t s;
            if (a[i].k == HV_NIL) {
                continue;
            }
            if (!hv_strval(st, &a[i], &s)) {
                return HV_UNKNOWN;
            }
            const char *q = fn[0] == 'q' ? hg_quote(st, s.s, s.sn, '"')
                                         : cbm_arena_sprintf(st->a, "'%.*s'", (int)s.sn, s.s);
            if (!q) {
                return HV_UNKNOWN;
            }
            if (written++) {
                hs_append(st, &out, " ", SKIP_ONE);
            }
            hs_append(st, &out, q, strlen(q));
        }
        return out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    if (strcmp(fn, "toString") == 0 && n == SKIP_ONE) {
        hv_t s;
        return hv_strval(st, &a[0], &s) ? s : HV_UNKNOWN;
    }
    if ((strcmp(fn, "int") == 0 || strcmp(fn, "int64") == 0 || strcmp(fn, "toInt") == 0) &&
        n == SKIP_ONE) {
        int64_t i = 0;
        return hv_to_int(&a[0], &i) ? hv_int(i) : HV_UNKNOWN;
    }
    if (strcmp(fn, "float64") == 0 && n == SKIP_ONE) {
        int64_t i = 0;
        return a[0].k == HV_FLOAT ? a[0] : hv_to_int(&a[0], &i) ? hv_float((double)i) : HV_UNKNOWN;
    }
    if (strcmp(fn, "len") == 0 && n == SKIP_ONE) {
        int l = hv_len(&a[0]);
        return l >= 0 ? hv_int(l) : HV_UNKNOWN;
    }
    if (strcmp(fn, "dict") == 0) {
        hv_map_t *m = cbm_arena_calloc(st->a, sizeof(*m));
        int pairs = (n + SKIP_ONE) / PAIR_LEN;
        if (!m) {
            return HV_UNKNOWN;
        }
        m->keys = pairs ? cbm_arena_alloc(st->a, (size_t)pairs * sizeof(*m->keys)) : NULL;
        m->vals = pairs ? cbm_arena_alloc(st->a, (size_t)pairs * sizeof(*m->vals)) : NULL;
        if (pairs && (!m->keys || !m->vals)) {
            return HV_UNKNOWN;
        }
        for (int i = 0; i + SKIP_ONE <= n; i += PAIR_LEN) {
            hv_t k;
            if (!hv_strval(st, &a[i], &k)) {
                return HV_UNKNOWN;
            }
            const char *key = cbm_arena_strndup(st->a, k.s, k.sn);
            hv_t val = i + SKIP_ONE < n ? a[i + SKIP_ONE] : hv_str("", 0);
            int at = m->n;
            for (int j = 0; j < m->n; j++) {
                if (strcmp(m->keys[j], key) == 0) {
                    at = j;
                }
            }
            m->keys[at] = key;
            m->vals[at] = val;
            m->n += at == m->n;
        }
        return (hv_t){.k = HV_MAP, .m = m};
    }
    if (strcmp(fn, "list") == 0) {
        return hv_mklist(st, a, n);
    }
    if (strcmp(fn, "get") == 0 && n == PAIR_LEN) {
        if (!hv_is_map(&a[0]) || a[1].k != HV_STR) {
            return HV_UNKNOWN;
        }
        hv_t v = he_field(st, &a[0], a[1].s, a[1].sn);
        return v.k == HV_NIL ? hv_str("", 0) : v;
    }
    if (strcmp(fn, "hasKey") == 0 && n == PAIR_LEN) {
        if (!hv_is_map(&a[0]) || a[1].k != HV_STR) {
            return HV_UNKNOWN;
        }
        if (a[0].k == HV_MAP) {
            for (int i = 0; i < a[0].m->n; i++) {
                if (strlen(a[0].m->keys[i]) == a[1].sn &&
                    memcmp(a[0].m->keys[i], a[1].s, a[1].sn) == 0) {
                    return hv_bool(true);
                }
            }
            return hv_bool(false);
        }
        hst_record(st, hst_path(st, a[0].path, a[1].s, a[1].sn));
        return hv_bool(yyjson_mut_obj_getn(a[0].j, a[1].s, a[1].sn) != NULL);
    }
    if (strcmp(fn, "index") == 0 && n >= SKIP_ONE) {
        hv_t v = a[0];
        for (int i = SKIP_ONE; i < n && v.k != HV_UNK; i++) {
            v = he_index1(st, &v, &a[i]);
        }
        return v;
    }
    if ((strcmp(fn, "first") == 0 || strcmp(fn, "last") == 0) && n == SKIP_ONE) {
        hv_t *items = NULL;
        int c = hv_items(st, &a[0], &items);
        if (c < 0) {
            return HV_UNKNOWN;
        }
        return c == 0 ? hv_nil() : items[fn[0] == 'f' ? 0 : c - SKIP_ONE];
    }
    if (strcmp(fn, "join") == 0 && n == PAIR_LEN) {
        hv_t *items = NULL;
        int c = hv_items(st, &a[1], &items);
        hv_t sep;
        if (c < 0 || !hv_strval(st, &a[0], &sep)) {
            return HV_UNKNOWN;
        }
        hsink_t out = {0};
        for (int i = 0; i < c; i++) {
            hv_t s;
            if (!hv_strval(st, &items[i], &s)) {
                return HV_UNKNOWN;
            }
            if (i) {
                hs_append(st, &out, sep.s, sep.sn);
            }
            hs_append(st, &out, s.s, s.sn);
        }
        return out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    if (strcmp(fn, "splitList") == 0 && n == PAIR_LEN && a[0].k == HV_STR && a[1].k == HV_STR &&
        a[0].sn > 0) {
        int cap = SKIP_ONE;
        for (size_t i = 0; i + a[0].sn <= a[1].sn; i++) {
            cap += memcmp(a[1].s + i, a[0].s, a[0].sn) == 0;
        }
        hv_t *items = cbm_arena_alloc(st->a, (size_t)cap * sizeof(*items));
        if (!items) {
            return HV_UNKNOWN;
        }
        int c = 0;
        size_t from = 0;
        for (size_t i = 0; i + a[0].sn <= a[1].sn;) {
            if (memcmp(a[1].s + i, a[0].s, a[0].sn) == 0) {
                items[c++] = hv_str(a[1].s + from, i - from);
                i += a[0].sn;
                from = i;
            } else {
                i++;
            }
        }
        items[c++] = hv_str(a[1].s + from, a[1].sn - from);
        return hv_mklist(st, items, c);
    }
    if ((strcmp(fn, "nindent") == 0 || strcmp(fn, "indent") == 0) && n == PAIR_LEN) {
        int64_t w = 0;
        hv_t s;
        if (!hv_to_int(&a[0], &w) || w < 0 || !hv_strval(st, &a[1], &s)) {
            return HV_UNKNOWN;
        }
        /* every line gets w spaces: a width whose output cannot fit stops here */
        size_t lines = SKIP_ONE;
        for (size_t i = 0; i < s.sn; i++) {
            lines += s.s[i] == '\n';
        }
        if ((uint64_t)w > HELM_MAX_OUTPUT / lines) {
            hst_trip(st, "output");
            return HV_UNKNOWN;
        }
        hsink_t out = {0};
        if (fn[0] == 'n') {
            hs_append(st, &out, "\n", SKIP_ONE);
        }
        for (int64_t i = 0; i < w && !out.unk; i++) {
            hs_append(st, &out, " ", SKIP_ONE);
        }
        for (size_t i = 0; i < s.sn && !out.unk; i++) {
            hs_append(st, &out, s.s + i, SKIP_ONE);
            for (int64_t k = 0; s.s[i] == '\n' && k < w && !out.unk; k++) {
                hs_append(st, &out, " ", SKIP_ONE);
            }
        }
        return out.unk ? HV_UNKNOWN : hv_str(hs_str(st, &out), out.n);
    }
    if ((strcmp(fn, "kindIs") == 0 && n == PAIR_LEN) || (strcmp(fn, "kindOf") == 0 && n == 1)) {
        const hv_t *v = &a[n - SKIP_ONE];
        const char *kind = v->k == HV_NIL     ? "invalid"
                           : v->k == HV_BOOL  ? "bool"
                           : v->k == HV_INT   ? (v->path ? "int64" : "int")
                           : v->k == HV_FLOAT ? "float64"
                           : v->k == HV_STR   ? "string"
                           : hv_is_map(v)     ? "map"
                           : hv_is_list(v)    ? "slice"
                                              : NULL;
        if (!kind) {
            return HV_UNKNOWN;
        }
        if (fn[4] == 'O') {
            return hv_str(kind, strlen(kind));
        }
        return a[0].k == HV_STR
                   ? hv_bool(strlen(kind) == a[0].sn && memcmp(kind, a[0].s, a[0].sn) == 0)
                   : HV_UNKNOWN;
    }
    if (strcmp(fn, "dig") == 0 && n >= 3) {
        hv_t v = a[n - SKIP_ONE];
        for (int i = 0; i < n - PAIR_LEN; i++) {
            if (!hv_is_map(&v) || a[i].k != HV_STR) {
                return HV_UNKNOWN;
            }
            v = he_field(st, &v, a[i].s, a[i].sn);
            if (v.k == HV_NIL) {
                return a[n - PAIR_LEN];
            }
        }
        return v;
    }
    if (strcmp(fn, "trim") == 0 || strcmp(fn, "trimPrefix") == 0 || strcmp(fn, "trimSuffix") == 0 ||
        strcmp(fn, "trimAll") == 0 || strcmp(fn, "lower") == 0 || strcmp(fn, "upper") == 0 ||
        strcmp(fn, "replace") == 0 || strcmp(fn, "trunc") == 0 || strcmp(fn, "contains") == 0 ||
        strcmp(fn, "hasPrefix") == 0 || strcmp(fn, "hasSuffix") == 0) {
        return he_strfn(st, fn, a, n);
    }
    (void)fr;
    return HV_UNKNOWN; /* not modelled (toYaml, lookup, regex, semver, merge, ...) */
}

/* A function call node -- `fn args...` or a bare `fn` -- with `piped` (may
 * be NULL) as its last argument. and / or evaluate lazily. */
static hv_t he_call(hst_t *st, hframe_t *fr, TSNode call, const hv_t *piped) {
    TSNode fnode = hn_is(call, "identifier") ? call : hn_field(call, "function");
    if (ts_node_is_null(fnode)) {
        return HV_UNKNOWN;
    }
    size_t fl = 0;
    const char *ft = hn_text(fr, fnode, &fl);
    char fn[CBM_SZ_64];
    if (fl >= sizeof(fn)) {
        return HV_UNKNOWN;
    }
    memcpy(fn, ft, fl);
    fn[fl] = '\0';
    TSNode args = hn_is(call, "identifier") ? (TSNode){0} : hn_field(call, "arguments");
    uint32_t na = ts_node_is_null(args) ? 0 : ts_node_named_child_count(args);
    if (he_mutator(fn, fl)) {
        /* set / unset / merge change a map in place -- not run: the map is
         * poisoned, what it holds UNKNOWN from here on (after a merge, every
         * map within it too). That is exact only for a known map (a merge:
         * whose every map within is known, see hst_poison); any other target
         * -- one not known, whatever gives it -- leaves every image after it
         * UNKNOWN. */
        TSNode first = na ? ts_node_named_child(args, 0) : (TSNode){0};
        hv_t target = na ? he_eval(st, fr, first) : piped ? *piped : HV_UNKNOWN;
        for (uint32_t i = SKIP_ONE; i < na; i++) {
            (void)he_eval(st, fr, ts_node_named_child(args, i)); /* what it runs */
        }
        if (!hv_is_map(&target) || !hst_poison(st, &target, fn[0] == 'm')) {
            hst_taint(st, fn);
        }
        return HV_UNKNOWN; /* the changed map */
    }
    bool lazy = strcmp(fn, "and") == 0 || strcmp(fn, "or") == 0;
    if (lazy) {
        hv_t v = HV_UNKNOWN;
        uint32_t total = na + (piped ? SKIP_ONE : 0);
        for (uint32_t i = 0; i < total; i++) {
            v = i < na ? he_eval(st, fr, ts_node_named_child(args, i)) : *piped;
            if (v.k == HV_UNK || hv_poisoned(st, &v)) {
                /* its truth is not known (a poisoned map's either): whether
                 * the operands after it run is not, nor a change they make */
                for (uint32_t j = i + SKIP_ONE; j < na; j++) {
                    if (he_may_mutate(st, fr->src, ts_node_named_child(args, j), st->nesting)) {
                        hst_taint(st, fn);
                        break;
                    }
                }
                return HV_UNKNOWN;
            }
            bool t = hv_truth(&v);
            if ((fn[0] == 'a' && !t) || (fn[0] == 'o' && t)) {
                return v;
            }
        }
        return total ? v : HV_UNKNOWN;
    }
    int n = (int)na + (piped ? SKIP_ONE : 0);
    hv_t *a = n ? cbm_arena_alloc(st->a, (size_t)n * sizeof(*a)) : NULL;
    if (n && !a) {
        return HV_UNKNOWN;
    }
    for (uint32_t i = 0; i < na; i++) {
        a[i] = he_eval(st, fr, ts_node_named_child(args, i));
    }
    if (piped) {
        a[na] = *piped;
    }
    return he_apply_fn(st, fr, fn, a, n);
}

static hv_t he_literal_int(hframe_t *fr, TSNode n) {
    size_t l = 0;
    const char *t = hn_text(fr, n, &l);
    char buf[CBM_SZ_64];
    size_t k = 0;
    for (size_t i = 0; i < l && k + SKIP_ONE < sizeof(buf); i++) {
        if (t[i] != '_') {
            buf[k++] = t[i];
        }
    }
    buf[k] = '\0';
    int base = 10;
    const char *p = buf;
    if (p[0] == '0' && (p[1] == 'o' || p[1] == 'O')) {
        base = 8;
        p += PAIR_LEN;
    } else if (p[0] == '0' && (p[1] == 'b' || p[1] == 'B')) {
        base = 2;
        p += PAIR_LEN;
    } else if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
        base = 16;
        p += PAIR_LEN;
    } else if (p[0] == '0' && p[1]) {
        base = 8;
        p++;
    }
    char *end = NULL;
    long long v = strtoll(p, &end, base);
    return end && !*end ? hv_int(v) : HV_UNKNOWN;
}

static hv_t he_eval(hst_t *st, hframe_t *fr, TSNode n) {
    if (ts_node_is_null(n) || ts_node_has_error(n) || ts_node_is_missing(n) || !hst_step(st)) {
        return HV_UNKNOWN;
    }
    const char *t = ts_node_type(n);
    size_t l = 0;
    if (strcmp(t, "dot") == 0) {
        return fr->dot;
    }
    if (strcmp(t, "field") == 0) {
        TSNode nm = hn_field(n, "name");
        if (ts_node_is_null(nm)) {
            return HV_UNKNOWN;
        }
        const char *name = hn_text(fr, nm, &l);
        return he_field(st, &fr->dot, name, l);
    }
    if (strcmp(t, "variable") == 0) {
        const char *name = hn_var_name(fr, n, &l);
        hvar_t *v = hst_var(st, fr, name, l);
        return v ? v->v : HV_UNKNOWN;
    }
    if (strcmp(t, "selector_expression") == 0) {
        TSNode op = hn_field(n, "operand");
        TSNode fd = hn_field(n, "field");
        if (ts_node_is_null(fd)) {
            return HV_UNKNOWN;
        }
        hv_t base = he_eval(st, fr, op);
        if (base.k == HV_UNK) {
            return HV_UNKNOWN;
        }
        const char *name = hn_text(fr, fd, &l);
        return he_field(st, &base, name, l);
    }
    if (strcmp(t, "chained_pipeline") == 0) {
        uint32_t c = ts_node_named_child_count(n);
        if (c == 0) {
            return HV_UNKNOWN;
        }
        hv_t v = he_eval(st, fr, ts_node_named_child(n, 0));
        for (uint32_t i = SKIP_ONE; i < c && v.k != HV_UNK; i++) {
            TSNode stage = ts_node_named_child(n, i);
            if (!hn_is(stage, "function_call") && !hn_is(stage, "identifier")) {
                return HV_UNKNOWN;
            }
            v = he_call(st, fr, stage, &v);
        }
        return v;
    }
    if (strcmp(t, "function_call") == 0 || strcmp(t, "identifier") == 0) {
        return he_call(st, fr, n, NULL);
    }
    if (strcmp(t, "parenthesized_pipeline") == 0) {
        return ts_node_named_child_count(n) == SKIP_ONE ? he_eval(st, fr, ts_node_named_child(n, 0))
                                                        : HV_UNKNOWN;
    }
    if (strcmp(t, "interpreted_string_literal") == 0 || strcmp(t, "raw_string_literal") == 0) {
        const char *s = hn_text(fr, n, &l);
        char *out = cbm_arena_alloc(st->a, l * 4 + SKIP_ONE);
        size_t k = out ? hg_unquote(s, l, out) : (size_t)-1;
        if (k == (size_t)-1) {
            return HV_UNKNOWN;
        }
        out[k] = '\0';
        return hv_str(out, k);
    }
    if (strcmp(t, "int_literal") == 0) {
        return he_literal_int(fr, n);
    }
    if (strcmp(t, "float_literal") == 0) {
        const char *s = hn_text(fr, n, &l);
        char *buf = cbm_arena_strndup(st->a, s, l);
        return buf ? hv_float(strtod(buf, NULL)) : HV_UNKNOWN;
    }
    if (strcmp(t, "true") == 0 || strcmp(t, "false") == 0) {
        return hv_bool(t[0] == 't');
    }
    if (strcmp(t, "nil") == 0) {
        return hv_nil();
    }
    if (strcmp(t, "variable_definition") == 0 || strcmp(t, "assignment") == 0) {
        TSNode var = hn_field(n, "variable");
        hv_t v = he_eval(st, fr, hn_field(n, "value"));
        const char *name = ts_node_is_null(var) ? NULL : hn_var_name(fr, var, &l);
        if (!name) {
            return HV_UNKNOWN;
        }
        if (t[0] == 'v') {
            hst_var_push(st, name, l, v);
        } else {
            hvar_t *cur = hst_var(st, fr, name, l);
            if (!cur) {
                return HV_UNKNOWN;
            }
            cur->v = v;
        }
        return v;
    }
    return HV_UNKNOWN; /* method calls, rune / imaginary literals, ... */
}

/* ── Execution ────────────────────────────────────────────────────── */

static void he_item(hst_t *st, hframe_t *fr, TSNode c);

/* The named children of `parent` in field `field` (NULL: every one). */
static void he_items(hst_t *st, hframe_t *fr, TSNode parent, const char *field) {
    uint32_t cc = ts_node_child_count(parent);
    for (uint32_t i = 0; i < cc && !st->brk && !st->cont && hst_step(st); i++) {
        TSNode c = ts_node_child(parent, i);
        if (!ts_node_is_named(c)) {
            continue;
        }
        const char *f = ts_node_field_name_for_child(parent, i);
        if (field ? !(f && strcmp(f, field) == 0) : f != NULL) {
            continue;
        }
        he_item(st, fr, c);
    }
}

/* How a part not run would leave the loop around it. */
enum { HL_NONE = 0, HL_CONTINUE = 1, HL_BREAK = 2 };

/* The variables of the frames around n that n assigns (`$x = ...`) become
 * UNKNOWN; returns how n would leave the loop around it (HL_*) -- by a break
 * / continue not inside a range of n's own (`in_range`: n is a range's
 * body). */
static int he_scan_skipped(hst_t *st, hframe_t *fr, TSNode n, bool in_range) {
    TSTreeCursor cur = ts_tree_cursor_new(n);
    int leave = HL_NONE;
    bool more = true;
    while (more && hst_step(st)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        const char *t = ts_node_type(c);
        if (strcmp(t, "assignment") == 0) {
            TSNode var = hn_field(c, "variable");
            size_t l = 0;
            const char *name = ts_node_is_null(var) ? NULL : hn_var_name(fr, var, &l);
            hvar_t *v = name ? hst_var(st, fr, name, l) : NULL;
            if (v) {
                v->v = HV_UNKNOWN;
            }
        } else if (strcmp(t, "break_action") == 0 || strcmp(t, "continue_action") == 0) {
            /* a range around it inside n (n itself included) is the one it leaves */
            bool own = in_range || hn_is(n, "range_action");
            for (TSNode p = c; !own && !ts_node_eq(p, n) && !ts_node_is_null(p);) {
                p = ts_node_parent(p);
                own = hn_is(p, "range_action");
            }
            if (!own) {
                leave |= t[0] == 'b' ? HL_BREAK : HL_CONTINUE;
            }
        }
        if (ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
    return leave;
}

/* The children of n but what was run -- its collection, its conditions up
 * to `ran` (the one run; an else if / else with after it would be) -- each:
 * the variables it would assign UNKNOWN, and a change of maps it may make
 * (he_may_mutate) leaves every image after it UNKNOWN; returns how it would
 * leave the loop. */
static int he_spoil_parts(hst_t *st, hframe_t *fr, TSNode n, TSNode ran, const char *only,
                          bool in_range) {
    int leave = HL_NONE;
    bool mutates = false;
    bool after = false; /* past `ran` */
    uint32_t cc = ts_node_child_count(n);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode c = ts_node_child(n, i);
        const char *f = ts_node_field_name_for_child(n, i);
        bool cond = f && strcmp(f, "condition") == 0;
        bool was_run = cond && !after;
        after = after || (cond && ts_node_eq(c, ran));
        if (!ts_node_is_named(c) || hn_is(c, "range_variable_definition") || was_run ||
            (f && strcmp(f, "range") == 0) || (only && !(f && strcmp(f, only) == 0))) {
            continue;
        }
        leave |= he_scan_skipped(st, fr, c, in_range);
        mutates = mutates || he_may_mutate(st, fr->src, c, st->nesting);
    }
    if (mutates) {
        hst_taint(st, "skipped");
    }
    return leave;
}

/* The bodies of if / with / range n, not run because its condition `ran` or
 * its collection (a null `ran`) is UNKNOWN (that one was run): their output,
 * the variables they -- and the conditions after `ran` -- would assign and a
 * change they would make are UNKNOWN. A break / continue in them may or may
 * not leave the loop around: what the rest of that loop's body would assign
 * or change is UNKNOWN too, and a break leaves it now (its later iterations
 * are not known), a continue this iteration. */
static void he_skipped(hst_t *st, hframe_t *fr, TSNode n, TSNode ran) {
    hst_out_unknown(st, fr);
    bool range = hn_is(n, "range_action");
    int leave = he_spoil_parts(st, fr, n, ran, NULL, range);
    if (leave == HL_NONE) {
        return;
    }
    TSNode loop = ts_node_parent(n);
    for (int depth = 0;
         !ts_node_is_null(loop) && !hn_is(loop, "range_action") && depth < HELM_MAX_NESTING;
         depth++) {
        loop = ts_node_parent(loop);
    }
    if (hn_is(loop, "range_action")) {
        (void)he_spoil_parts(st, fr, loop, (TSNode){0}, "body", true);
    }
    if (leave & HL_BREAK) {
        st->brk = true;
    } else {
        st->cont = true;
    }
}

/* if / else if / else, and with / else with: the first branch whose
 * condition holds runs; an UNKNOWN condition runs none. */
static void he_branches(hst_t *st, hframe_t *fr, TSNode n, bool with) {
    uint32_t cc = ts_node_child_count(n);
    bool taken = false;
    bool stop = false;
    bool active = false;
    int mark = st->nvars;
    hframe_t sub = *fr;
    for (uint32_t i = 0; i < cc && !st->brk && !st->cont; i++) {
        TSNode c = ts_node_child(n, i);
        if (!ts_node_is_named(c)) {
            continue;
        }
        const char *f = ts_node_field_name_for_child(n, i);
        if (!f) {
            continue;
        }
        if (strcmp(f, "condition") == 0) {
            if (taken || stop) {
                active = false;
                continue;
            }
            hv_t v = he_eval(st, fr, c);
            if (v.k == HV_UNK || hv_poisoned(st, &v)) {
                stop = true;
                active = false;
                he_skipped(st, fr, n, c); /* which branch renders is unknown */
                continue;
            }
            active = hv_truth(&v);
            taken = active;
            sub = *fr;
            if (with && active) {
                sub.dot = v;
            }
        } else if (strcmp(f, "consequence") == 0 || strcmp(f, "option") == 0) {
            if (active) {
                he_item(st, &sub, c);
            }
        } else if (strcmp(f, "alternative") == 0) {
            if (!taken && !stop) {
                he_item(st, fr, c);
            }
        }
    }
    st->nvars = mark;
}

static void he_range(hst_t *st, hframe_t *fr, TSNode n) {
    TSNode def = {0};
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        if (hn_is(ts_node_named_child(n, i), "range_variable_definition")) {
            def = ts_node_named_child(n, i);
        }
    }
    TSNode expr = ts_node_is_null(def) ? hn_field(n, "range") : hn_field(def, "range");
    TSNode ivar = ts_node_is_null(def) ? (TSNode){0} : hn_field(def, "index");
    TSNode evar = ts_node_is_null(def) ? (TSNode){0} : hn_field(def, "element");
    if (!ts_node_is_null(ivar) && ts_node_is_null(evar)) {
        evar = ivar; /* range $x := ...: the element */
        ivar = (TSNode){0};
    }
    hv_t coll = he_eval(st, fr, expr);
    if (coll.k == HV_UNK || hv_poisoned(st, &coll)) {
        he_skipped(st, fr, n, (TSNode){0});
        return;
    }
    hv_t *items = NULL;
    const char **keys = NULL;
    int64_t count = 0;
    bool is_map = hv_is_map(&coll);
    if (is_map) {
        count = hv_keys(st, &coll, &keys);
    } else if (coll.k == HV_INT) {
        count = coll.i > 0 ? coll.i : 0;
    } else {
        count = hv_items(st, &coll, &items);
    }
    if (count < 0) {
        hst_out_unknown(st, fr);
        return;
    }
    size_t il = 0;
    size_t el = 0;
    const char *iname = ts_node_is_null(ivar) ? NULL : hn_var_name(fr, ivar, &il);
    const char *ename = ts_node_is_null(evar) ? NULL : hn_var_name(fr, evar, &el);
    for (int64_t k = 0; k < count && hst_step(st); k++) {
        int mark = st->nvars;
        hv_t elem;
        hv_t index;
        if (is_map) {
            elem = he_field(st, &coll, keys[k], strlen(keys[k]));
            index = hv_str(keys[k], strlen(keys[k]));
        } else if (coll.k == HV_INT) {
            elem = hv_int(k);
            index = hv_int(k);
        } else {
            elem = items[k];
            index = hv_int(k);
        }
        if (iname) {
            hst_var_push(st, iname, il, index);
        }
        if (ename) {
            hst_var_push(st, ename, el, elem);
        }
        hframe_t sub = *fr;
        sub.dot = elem;
        he_items(st, &sub, n, "body");
        st->nvars = mark;
        st->cont = false;
        if (st->brk) {
            st->brk = false;
            break;
        }
    }
    if (count == 0) {
        he_items(st, fr, n, "alternative");
    }
}

/* `template "name" arg` / a block: the define rendered into the output. */
static void he_template(hst_t *st, hframe_t *fr, TSNode n) {
    TSNode nm = hn_field(n, "name");
    TSNode arg = hn_field(n, "argument");
    size_t l = 0;
    const char *s = ts_node_is_null(nm) ? NULL : hn_text(fr, nm, &l);
    char *name = s ? cbm_arena_alloc(st->a, l * 4 + SKIP_ONE) : NULL;
    size_t k = name ? hg_unquote(s, l, name) : (size_t)-1;
    if (k == (size_t)-1) {
        hst_out_unknown(st, fr);
        return;
    }
    name[k] = '\0';
    hv_t a = ts_node_is_null(arg) ? hv_nil() : he_eval(st, fr, arg);
    if (a.k == HV_UNK || hv_poisoned(st, &a)) {
        hst_out_unknown(st, fr);
        /* the define not run: a change of maps it may make is missed */
        if (he_define_may_mutate(st, name, st->nesting)) {
            hst_taint(st, "template");
        }
        return;
    }
    if (fr->out) {
        if (he_render_define(st, name, a, fr->out).k == HV_UNK) {
            fr->out->unk = true;
        }
        return;
    }
    /* at the top level the define's text is output that probes may read */
    hsink_t out = {0};
    if (he_render_define(st, name, a, &out).k == HV_UNK || out.unk) {
        hst_out_unknown(st, fr);
        return;
    }
    hst_out(st, fr, out.buf ? out.buf : "", out.n);
}

static void he_item(hst_t *st, hframe_t *fr, TSNode c) {
    const char *t = ts_node_type(c);
    if (strcmp(t, "text") == 0) {
        hst_text(st, fr, c);
    } else if (strcmp(t, "comment") == 0 || strcmp(t, "define_action") == 0) {
        return;
    } else if (strcmp(t, "if_action") == 0) {
        he_branches(st, fr, c, false);
    } else if (strcmp(t, "with_action") == 0) {
        he_branches(st, fr, c, true);
    } else if (strcmp(t, "range_action") == 0) {
        he_range(st, fr, c);
    } else if (strcmp(t, "template_action") == 0 || strcmp(t, "block_action") == 0) {
        he_template(st, fr, c);
    } else if (strcmp(t, "break_action") == 0) {
        st->brk = true;
    } else if (strcmp(t, "continue_action") == 0) {
        st->cont = true;
    } else if (strcmp(t, "variable_definition") == 0 || strcmp(t, "assignment") == 0) {
        (void)he_eval(st, fr, c);
    } else {
        hv_t v = he_eval(st, fr, c);
        if (v.k == HV_UNK) {
            hst_out_unknown(st, fr);
        } else {
            hst_out_value(st, fr, &v);
        }
    }
}

/* Define `name` rendered with dot / $ = arg into `into`. */
static hv_t he_render_define(hst_t *st, const char *name, hv_t arg, hsink_t *into) {
    const hd_def_t *d = hd_find(st->defs, name);
    if (!d) {
        /* not run -- a template file of the chart by its name (the checksum
         * idiom) or a name not the chart's: its output is UNKNOWN, and a
         * change it may make (he_define_may_mutate) is missed */
        if (he_define_may_mutate(st, name, st->nesting)) {
            hst_taint(st, "include");
        }
        return HV_UNKNOWN;
    }
    if (++st->nesting > HELM_MAX_NESTING) {
        st->nesting--;
        hst_taint(st, "include"); /* nested too deep: not run */
        return HV_UNKNOWN;
    }
    if (ts_node_has_error(d->node)) {
        hst_taint(st, "parse"); /* a form the grammar rejects may hide a change */
    }
    int mark = st->nvars;
    hframe_t sub = {.src = d->file->src, .dot = arg, .var_base = st->nvars, .out = into};
    hst_var_push(st, "", 0, arg);
    bool brk = st->brk;
    bool cont = st->cont;
    st->brk = false;
    st->cont = false;
    he_items(st, &sub, d->node, "body");
    st->brk = brk;
    st->cont = cont;
    st->nvars = mark;
    st->nesting--;
    return into->unk ? HV_UNKNOWN : hv_nil();
}

/* ── Entry ────────────────────────────────────────────────────────── */

static hv_map_t *hv_struct(hst_t *st, int n) {
    hv_map_t *m = cbm_arena_calloc(st->a, sizeof(*m));
    if (m) {
        m->keys = cbm_arena_alloc(st->a, (size_t)n * sizeof(*m->keys));
        m->vals = cbm_arena_alloc(st->a, (size_t)n * sizeof(*m->vals));
        m->strict = true;
        if (!m->keys || !m->vals) {
            return NULL;
        }
    }
    return m;
}

static void hv_struct_set(hv_map_t *m, const char *k, hv_t v) {
    m->keys[m->n] = k;
    m->vals[m->n++] = v;
}

static hv_t hv_cstr(const char *s) {
    return s ? hv_str(s, strlen(s)) : hv_nil();
}

/* The probes' captures, made in the render's own arena, copied into a: the
 * caller keeps a render's results, never its work. An image whose mark is not
 * copied is not kept either. */
static void helm_keep_results(CBMArena *a, cbm_helm_probe_t *probes, int nprobes) {
    for (int i = 0; i < nprobes; i++) {
        cbm_helm_probe_t *p = &probes[i];
        int n = p->n;
        const char **img = n ? cbm_arena_alloc(a, (size_t)n * sizeof(*img)) : NULL;
        const char **keys = n ? cbm_arena_alloc(a, (size_t)n * sizeof(*keys)) : NULL;
        if (!img || !keys) {
            n = 0;
            img = NULL;
            keys = NULL;
        }
        for (int c = 0; c < n; c++) {
            const char *k = p->keys[c] ? cbm_arena_strdup(a, p->keys[c]) : NULL;
            img[c] = p->image[c] && (k || !p->keys[c]) ? cbm_arena_strdup(a, p->image[c]) : NULL;
            keys[c] = k;
        }
        p->image = img;
        p->keys = keys;
        p->n = n;
        p->cap = n;
    }
}

bool cbm_helm_render(CBMArena *a, const char *rel, const char *src, size_t len,
                     yyjson_mut_val *values, const cbm_helm_chart_meta_t *chart,
                     const cbm_helm_defines_t *defs, cbm_helm_probe_t *probes, int nprobes) {
    for (int i = 0; i < nprobes; i++) {
        probes[i].n = 0;
        probes[i].image = NULL;
        probes[i].keys = NULL;
        probes[i].cap = 0;
    }
    if (!src || nprobes == 0) {
        return true;
    }
    /* the render's work lives in its own arena, freed when it ends: the
     * memory budget bounds one render, and no render keeps another's */
    CBMArena work;
    cbm_arena_init_lazy(&work, CBM_SZ_4K);
    hst_t st = {.a = &work,
                .defs = defs,
                .top_src = src,
                .top_len = len,
                .probes = probes,
                .nprobes = nprobes,
                .rel = rel,
                .base_path = chart ? chart->base_path : NULL,
                .chart_dir = chart ? chart->dir : NULL,
                .capture = -SKIP_ONE,
                .arena0 = cbm_arena_total(&work)};
    st.scratch = yyjson_mut_doc_new(cbm_json_writer_allocator());
    st.parser = ts_parser_new();
    if (!st.scratch || !st.parser ||
        !ts_parser_set_language(st.parser, cbm_ts_language(CBM_LANG_GOTEMPLATE))) {
        if (st.parser) {
            ts_parser_delete(st.parser);
        }
        if (st.scratch) {
            yyjson_mut_doc_free(st.scratch);
        }
        cbm_arena_destroy(&work);
        return true;
    }
    int nl = SKIP_ONE;
    for (size_t i = 0; i < len; i++) {
        nl += src[i] == '\n';
    }
    uint32_t *lines = cbm_arena_alloc(&work, (size_t)nl * sizeof(*lines));
    TSTree *tree = lines ? ts_parser_parse_string(st.parser, NULL, src, (uint32_t)len) : NULL;
    if (tree) {
        /* a template that does not parse may hide a change (Go may take a
         * form the grammar rejects): no image of it is known */
        if (ts_node_has_error(ts_tree_root_node(tree))) {
            hst_taint(&st, "parse");
        }
        int k = 0;
        lines[k++] = 0;
        for (size_t i = 0; i < len; i++) {
            if (src[i] == '\n' && k < nl) {
                lines[k++] = (uint32_t)(i + SKIP_ONE);
            }
        }
        st.lines = lines;
        st.nlines = k;
        yyjson_mut_val *vals = values ? values : yyjson_mut_obj(st.scratch);
        hv_map_t *ch = hv_struct(&st, 3);
        hv_map_t *rl = hv_struct(&st, SKIP_ONE);
        hv_map_t *tp = st.base_path ? hv_struct(&st, SKIP_ONE) : NULL;
        hv_map_t *root = hv_struct(&st, PAIR_LEN + PAIR_LEN);
        if (ch && rl && root) {
            hv_struct_set(ch, "Name", hv_cstr(chart ? chart->name : NULL));
            hv_struct_set(ch, "Version", hv_cstr(chart ? chart->version : NULL));
            hv_struct_set(ch, "AppVersion", hv_cstr(chart ? chart->app_version : NULL));
            hv_struct_set(rl, "Service", hv_cstr("Helm"));
            hv_struct_set(root, "Values", hv_json(vals, ""));
            hv_struct_set(root, "Chart", (hv_t){.k = HV_MAP, .m = ch});
            hv_struct_set(root, "Release", (hv_t){.k = HV_MAP, .m = rl});
            if (tp) {
                /* .Template.Name is not modelled: UNKNOWN */
                hv_struct_set(tp, "BasePath", hv_cstr(st.base_path));
                hv_struct_set(root, "Template", (hv_t){.k = HV_MAP, .m = tp});
            }
            hv_t dot = {.k = HV_MAP, .m = root};
            hframe_t fr = {.src = src, .dot = dot, .var_base = 0, .out = NULL};
            hst_var_push(&st, "", 0, dot);
            he_items(&st, &fr, ts_tree_root_node(tree), NULL);
            hst_capture_end(&st);
        }
        ts_tree_delete(tree);
    }
    for (int i = 0; i < st.ntrees; i++) {
        ts_tree_delete(st.trees[i]);
    }
    ts_parser_delete(st.parser);
    yyjson_mut_doc_free(st.scratch);
    if (st.mutates) {
        cbm_ht_free(st.mutates);
    }
    if (st.file_mutates) {
        cbm_ht_free(st.file_mutates);
    }
    if (st.poison_maps) {
        cbm_ht_free(st.poison_maps);
    }
    if (st.poison_paths) {
        cbm_ht_free(st.poison_paths);
    }
    if (st.ckset) {
        cbm_ht_free(st.ckset);
    }
    if (st.stop) {
        /* Helm would not finish this template: none of its images is known,
         * also those captured before the budget ran out */
        for (int i = 0; i < nprobes; i++) {
            for (int c = 0; c < probes[i].n; c++) {
                probes[i].image[c] = NULL;
                probes[i].keys[c] = NULL;
            }
        }
        cbm_log_warn("helm.render.budget", "file", rel ? rel : "", "reason",
                     st.why ? st.why : "memory");
    }
    helm_keep_results(a, probes, nprobes);
    cbm_arena_destroy(&work);
    return !st.stop;
}
