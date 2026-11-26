/*
 * doc_pdf.c — PDF documents in the graph.
 *
 * A PDF's text layer (pdf/) becomes one Section per page: name "page N", the
 * page's text as its docstring, so search finds what a page says. The code a
 * page names becomes doc-link tokens for the resolver (doc_links_pdf.c):
 * STRUCTURAL mentions only -- paths ("src/auth/handler.go", "./x/y",
 * "Pool.sol") and qualified names ("pkg.Type.method", "a::b", "x->y"). Bare
 * identifiers never become tokens (field test E6c: bare names stay
 * suggestions).
 *
 * The scanner is the field-tested one (E6 idents.py), ported with CPython's
 * semantics: NFKC per code point plus canonical composition, the re module's
 * leftmost-first matching, str predicates from the same Unicode tables
 * (pdf/pdf_unicode.c). A mention split across two lines ("src/auth/" /
 * "handler.go", "Handler::" / "run") is proposed as a JOIN token that lists
 * the line-local tokens it would replace; the resolver keeps the join only
 * when it resolves EXACT or UNIQUE, and the fragments stand otherwise.
 *
 * Hygiene from the field test, applied here where the text decides it: R1 a
 * PDF under a test or fixture directory gives no tokens; R3 a mention written
 * as a keyword argument ("(x, foo.bar=1)") is no reference.
 */
#include "doclink.h"

#include "arena.h"
#include "cbm.h"
#include "foundation/mem_core.h"
#include "helpers.h"
#include "pdf/pdf.h"
#include "pdf/pdf_unicode.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* candidates a join compared, for the cost test */
static atomic_size_t g_join_steps;
#endif

enum {
    PDF_R3_WINDOW = 400, /* the field test looked this far back for "(" or "," */
    PDF_PAGE_NAME = 32,
    PDF_LIST_MIN = 64,
    PDF_U8_MAX = 4,
    PDF_PAGE_DOC_MAX = 256 * 1024, /* bytes of a page's text kept as its docstring */
};

#define PDF_JOIN_SEP '\x1f'  /* between a join's forms */
#define PDF_JOIN_FRAG '\x1e' /* before a join's fragment token indices */

/* ── Word lists (the field test's, verbatim) ─────────────────────── */

static const char *const CODE_EXT[] = {
    "py",    "pyi",     "pyx",    "pxd",    "c",    "h",     "cc",      "cpp",   "cxx",   "hpp",
    "hh",    "hxx",     "cs",     "csx",    "java", "kt",    "kts",     "scala", "go",    "rs",
    "rb",    "php",     "js",     "jsx",    "mjs",  "cjs",   "ts",      "tsx",   "swift", "m",
    "mm",    "sol",     "sh",     "bash",   "zsh",  "ps1",   "psm1",    "lua",   "r",     "jl",
    "dart",  "ex",      "exs",    "erl",    "hs",   "ml",    "fs",      "fsx",   "vb",    "sql",
    "proto", "graphql", "gql",    "thrift", "yaml", "yml",   "json",    "toml",  "ini",   "cfg",
    "conf",  "xml",     "html",   "htm",    "css",  "scss",  "less",    "md",    "rst",   "txt",
    "adoc",  "ipynb",   "gradle", "csproj", "sln",  "props", "targets", "cmake", "mk",    "tf",
    "hcl",   "bzl",     "lock",   "env",    "pdf",  "vue",   "svelte",  "tex",   "bib",   "jsonl",
    "csv",   "tsv",     "pom",    "jar",    "war",  "dll",   "so",      "wasm",  "zig",   "nim",
    "v",     NULL};

static const char *const TLDS[] = {"com", "org",   "net",  "io",   "dev", "edu", "gov", "de", "uk",
                                   "ai",  "co",    "info", "app",  "eu",  "fr",  "ch",  "us", "ca",
                                   "au",  "jp",    "cn",   "ru",   "nl",  "se",  "no",  "fi", "it",
                                   "es",  "at",    "be",   "biz",  "me",  "tv",  "ly",  "gg", "sh",
                                   "xyz", "cloud", "tech", "blog", NULL};

static const char *const SLASH_STOP[] = {
    "and/or",     "or/and", "input/output",  "i/o",     "read/write", "yes/no", "on/off",
    "true/false", "he/she", "his/her",       "s/he",    "w/o",        "n/a",    "km/h",
    "m/s",        "tcp/ip", "client/server", "him/her", "either/or",  NULL};

/* the field test's FIX: directories whose PDFs are fixtures (R1) */
static const char *const FIXTURE_DIRS[] = {"test",      "tests",     "cypress", "baseline_images",
                                           "resources", "samples",   "e2e",     "fixtures",
                                           "testdata",  "test_data", "data",    "examples",
                                           NULL};

/* ── Code-point buffers ──────────────────────────────────────────── */

typedef struct {
    uint32_t *v;
    int n;
    int cap;
    bool fail;
} u32v_t;

static bool u32_push(u32v_t *b, uint32_t c) {
    if (b->fail) {
        return false;
    }
    if (b->n == b->cap) {
        int ncap = b->cap ? b->cap * 2 : PDF_LIST_MIN;
        uint32_t *g =
            (uint32_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, b->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            b->fail = true;
            return false;
        }
        b->v = g;
        b->cap = ncap;
    }
    b->v[b->n++] = c;
    return true;
}

static void u32_free(u32v_t *b) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, b->v);
    memset(b, 0, sizeof(*b));
}

/* ── normalise(): NFKC, zero-width characters dropped, hyphens unified ── */

static const pdf_nfkc_t *nfkc_find(uint32_t cp) {
    int lo = 0;
    int hi = PDF_NFKC_COUNT - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (PDF_NFKC[mid].cp < cp) {
            lo = mid + 1;
        } else if (PDF_NFKC[mid].cp > cp) {
            hi = mid - 1;
        } else {
            return &PDF_NFKC[mid];
        }
    }
    return NULL;
}

static bool ucomp(uint32_t a, uint32_t b, uint32_t *out) {
    /* Hangul, algorithmically */
    enum {
        SBASE = 0xAC00,
        LBASE = 0x1100,
        VBASE = 0x1161,
        TBASE = 0x11A7,
        LCOUNT = 19,
        VCOUNT = 21,
        TCOUNT = 28,
        NCOUNT = VCOUNT * TCOUNT,
        SCOUNT = LCOUNT * NCOUNT
    };
    if (a >= LBASE && a < LBASE + LCOUNT && b >= VBASE && b < VBASE + VCOUNT) {
        *out = SBASE + ((a - LBASE) * VCOUNT + (b - VBASE)) * TCOUNT;
        return true;
    }
    if (a >= SBASE && a < SBASE + SCOUNT && (a - SBASE) % TCOUNT == 0 && b > TBASE &&
        b < TBASE + TCOUNT) {
        *out = a + (b - TBASE);
        return true;
    }
    int lo = 0;
    int hi = PDF_UCOMP_COUNT - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        const pdf_ucomp_t *e = &PDF_UCOMP[mid];
        if (e->first < a || (e->first == a && e->second < b)) {
            lo = mid + 1;
        } else if (e->first > a || e->second > b) {
            hi = mid - 1;
        } else {
            *out = e->composite;
            return true;
        }
    }
    return false;
}

static bool normalise(const char *s, size_t n, u32v_t *out) {
    size_t i = 0;
    while (i < n) {
        unsigned char c = (unsigned char)s[i];
        uint32_t cp = c;
        size_t k = 1;
        if (c >= 0xF0 && i + 3 < n) {
            cp = ((uint32_t)(c & 0x07) << 18) | ((uint32_t)(s[i + 1] & 0x3F) << 12) |
                 ((uint32_t)(s[i + 2] & 0x3F) << 6) | (uint32_t)(s[i + 3] & 0x3F);
            k = 4;
        } else if (c >= 0xE0 && i + 2 < n) {
            cp = ((uint32_t)(c & 0x0F) << 12) | ((uint32_t)(s[i + 1] & 0x3F) << 6) |
                 (uint32_t)(s[i + 2] & 0x3F);
            k = 3;
        } else if (c >= 0xC0 && i + 1 < n) {
            cp = ((uint32_t)(c & 0x1F) << 6) | (uint32_t)(s[i + 1] & 0x3F);
            k = 2;
        }
        i += k;
        const pdf_nfkc_t *m = nfkc_find(cp);
        int cnt = m ? m->len : 1;
        for (int j = 0; j < cnt; j++) {
            uint32_t x = m ? PDF_NFKC_SEQ[m->off + j] : cp;
            uint32_t comp;
            if (out->n > 0 && ucomp(out->v[out->n - 1], x, &comp)) {
                out->v[out->n - 1] = comp;
            } else if (!u32_push(out, x)) {
                return false;
            }
        }
    }
    /* zero-width characters out; U+2010/U+2011 -> '-' */
    int k = 0;
    for (int j = 0; j < out->n; j++) {
        uint32_t x = out->v[j];
        if (x == 0x200B || x == 0x200C || x == 0x200D || x == 0x2060 || x == 0xFEFF) {
            continue;
        }
        if (x == 0x2010 || x == 0x2011) {
            x = '-';
        }
        out->v[k++] = x;
    }
    out->n = k;
    return true;
}

/* ── Character classes of the field test's regexes ───────────────── */

static bool is_alnum_ascii(uint32_t c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

static bool is_alpha_ascii(uint32_t c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

/* [A-Za-z0-9_.$@\-] */
static bool path_seg(uint32_t c) {
    return is_alnum_ascii(c) || c == '_' || c == '.' || c == '$' || c == '@' || c == '-';
}

/* [A-Za-z0-9_.\-] */
static bool path_tail(uint32_t c) {
    return is_alnum_ascii(c) || c == '_' || c == '.' || c == '-';
}

/* [A-Za-z0-9_] */
static bool path_end(uint32_t c) {
    return is_alnum_ascii(c) || c == '_';
}

/* [A-Za-z_$] */
static bool ident_start(uint32_t c) {
    return is_alpha_ascii(c) || c == '_' || c == '$';
}

/* [\w$] */
static bool ident_cont(uint32_t c) {
    return pdf_u_word(c) || c == '$';
}

/* [A-Za-z0-9_$] (IDC) */
static bool idc(uint32_t c) {
    return is_alnum_ascii(c) || c == '_' || c == '$';
}

/* ── Candidates ──────────────────────────────────────────────────── */

typedef enum {
    MK_PATH = 0,
    MK_PATH1,
    MK_FILE,
    MK_QUAL,
    MK_SNAKE,
    MK_CAMEL,
    MK_CALL,
} mention_kind_t;

typedef struct {
    int start;
    int end;
    mention_kind_t kind;
    int norm_start; /* norm = a rewrite of the surface, kept in a pool */
    int norm_len;
} cand_t;

typedef struct {
    cand_t *v;
    int n;
    int cap;
    bool fail;
    u32v_t pool; /* norms */
} candv_t;

static bool cand_push(candv_t *cv, cand_t c) {
    if (cv->fail) {
        return false;
    }
    if (cv->n == cv->cap) {
        int ncap = cv->cap ? cv->cap * 2 : PDF_LIST_MIN;
        cand_t *g = (cand_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, cv->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            cv->fail = true;
            return false;
        }
        cv->v = g;
        cv->cap = ncap;
    }
    cv->v[cv->n++] = c;
    return true;
}

static void candv_free(candv_t *cv) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, cv->v);
    u32_free(&cv->pool);
    memset(cv, 0, sizeof(*cv));
}

static bool span_free(const uint8_t *taken, int a, int b) {
    for (int i = a; i < b; i++) {
        if (taken[i]) {
            return false;
        }
    }
    return true;
}

static void span_take(uint8_t *taken, int a, int b) {
    memset(taken + a, 1, (size_t)(b - a));
}

/* URL_RE: (?:https?|ftp|file)://\S+ | www\.\S+ | \S+@\S+\.[A-Za-z]{2,} */
typedef struct {
    int run_start; /* the non-space run the email alternative last looked at */
    int run_end;
    int at;     /* its last '@' with a domain after it, or -1 */
    int at_end; /* where that match ends */
} url_cache_t;

static bool lit_at(const uint32_t *t, int n, int p, const char *lit) {
    for (int i = 0; lit[i]; i++) {
        if (p + i >= n || t[p + i] != (uint32_t)(unsigned char)lit[i]) {
            return false;
        }
    }
    return true;
}

static int run_end_from(const uint32_t *t, int n, int p) {
    while (p < n && !pdf_u_space(t[p])) {
        p++;
    }
    return p;
}

static void url_cache_run(const uint32_t *t, int n, int p, url_cache_t *uc) {
    if (p >= uc->run_start && p < uc->run_end) {
        return; /* the run is known: validity of an '@' does not depend on p */
    }
    /* the scan enters a run at its start (every earlier position of a run
     * filled this cache); scanning back is only for a caller that did not */
    int rs = p;
    while (rs > 0 && !pdf_u_space(t[rs - 1])) {
        rs--;
    }
    uc->run_start = rs;
    uc->run_end = run_end_from(t, n, p);
    uc->at = -1;
    uc->at_end = -1;
    int re = uc->run_end;
    /* the largest '.' followed by two ASCII letters, scanning from the right;
     * for each '@' (right to left) the domain is the largest such '.' >= at+2 */
    int best_dot = -1;
    for (int a = re - 1; a > rs; a--) {
        if (a + 2 < re && t[a] == '.' && is_alpha_ascii(t[a + 1]) && is_alpha_ascii(t[a + 2])) {
            if (best_dot < 0) {
                best_dot = a;
            }
        }
        if (t[a] == '@' && best_dot >= a + 2) {
            int e = best_dot + 1;
            while (e < re && is_alpha_ascii(t[e])) {
                e++;
            }
            uc->at = a;
            uc->at_end = e;
            return;
        }
    }
}

static int url_at(const uint32_t *t, int n, int p, url_cache_t *uc) {
    if (lit_at(t, n, p, "https://") || lit_at(t, n, p, "http://") || lit_at(t, n, p, "ftp://") ||
        lit_at(t, n, p, "file://")) {
        int q = p;
        while (t[q] != '/') {
            q++;
        }
        q += 2;
        if (q < n && !pdf_u_space(t[q])) {
            return run_end_from(t, n, q);
        }
    }
    if (lit_at(t, n, p, "www.") && p + 4 < n && !pdf_u_space(t[p + 4])) {
        return run_end_from(t, n, p + 4);
    }
    if (pdf_u_space(t[p])) {
        return -1;
    }
    url_cache_run(t, n, p, uc);
    if (uc->at > p) {
        return uc->at_end;
    }
    return -1;
}

/* PATH_RE's body after the optional prefix: (?:[seg]+/)+[tail]*[end] */
static int path_body(const uint32_t *t, int n, int q) {
    int p = q;
    int last_ts = -1;
    for (;;) {
        int r = p;
        while (r < n && path_seg(t[r])) {
            r++;
        }
        if (r > p && r < n && t[r] == '/') {
            last_ts = r + 1;
            p = r + 1;
            continue;
        }
        break;
    }
    if (last_ts < 0) {
        return -1;
    }
    /* tail starts, from the last segment back to the first */
    int ts = last_ts;
    for (;;) {
        int r = ts;
        int last_e = -1;
        while (r < n && path_tail(t[r])) {
            if (path_end(t[r])) {
                last_e = r;
            }
            r++;
        }
        if (last_e >= 0) {
            return last_e + 1;
        }
        /* give back one segment: the previous '/' before ts-1 */
        int s = ts - 2;
        while (s >= q && t[s] != '/') {
            s--;
        }
        if (s < q) {
            return -1;
        }
        ts = s + 1;
    }
}

static int path_at(const uint32_t *t, int n, int p) {
    if (p > 0) {
        uint32_t b = t[p - 1];
        if (pdf_u_word(b) || b == '/' || b == '.' || b == '-' || b == '~' || b == '$' || b == '@') {
            return -1;
        }
    }
    /* (?:\.{1,2}/|~/|/)? tried in order, then without */
    int e;
    if (p + 2 < n && t[p] == '.' && t[p + 1] == '.' && t[p + 2] == '/' &&
        (e = path_body(t, n, p + 3)) >= 0) {
        return e;
    }
    if (p + 1 < n && t[p] == '.' && t[p + 1] == '/' && (e = path_body(t, n, p + 2)) >= 0) {
        return e;
    }
    if (p + 1 < n && t[p] == '~' && t[p + 1] == '/' && (e = path_body(t, n, p + 2)) >= 0) {
        return e;
    }
    if (t[p] == '/' && (e = path_body(t, n, p + 1)) >= 0) {
        return e;
    }
    return path_body(t, n, p);
}

/* QUAL_RE: [A-Za-z_$][\w$]*(?:(?:\.|::|->|#)[A-Za-z_$][\w$]*)+(?:\(\))? */
static int qual_at(const uint32_t *t, int n, int p) {
    if (p > 0) {
        uint32_t b = t[p - 1];
        if (pdf_u_word(b) || b == '.' || b == '$' || b == '@' || b == '/' || b == '-') {
            return -1;
        }
    }
    if (!ident_start(t[p])) {
        return -1;
    }
    int q = p + 1;
    while (q < n && ident_cont(t[q])) {
        q++;
    }
    int groups = 0;
    for (;;) {
        int sep = 0;
        if (q < n && t[q] == '.') {
            sep = 1;
        } else if (q + 1 < n && t[q] == ':' && t[q + 1] == ':') {
            sep = 2;
        } else if (q + 1 < n && t[q] == '-' && t[q + 1] == '>') {
            sep = 2;
        } else if (q < n && t[q] == '#') {
            sep = 1;
        }
        if (!sep || q + sep >= n || !ident_start(t[q + sep])) {
            break;
        }
        q += sep + 1;
        while (q < n && ident_cont(t[q])) {
            q++;
        }
        groups++;
    }
    if (!groups) {
        return -1;
    }
    if (q + 1 < n && t[q] == '(' && t[q + 1] == ')') {
        q += 2;
    }
    return q;
}

/* TOK_RE: [A-Za-z_$][\w$]* (whole identifier) */
static int tok_at(const uint32_t *t, int n, int p) {
    if (p > 0) {
        uint32_t b = t[p - 1];
        if (pdf_u_word(b) || b == '.' || b == '$' || b == '@' || b == '/' || b == '-') {
            return -1;
        }
    }
    if (!ident_start(t[p])) {
        return -1;
    }
    int q = p + 1;
    while (q < n && ident_cont(t[q])) {
        q++;
    }
    return q;
}

static uint32_t ascii_lower(uint32_t c) {
    return (c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c;
}

static bool in_list_lower(const uint32_t *s, int n, const char *const *list) {
    for (int i = 0; list[i]; i++) {
        const char *w = list[i];
        int k = 0;
        while (k < n && w[k] && ascii_lower(s[k]) == (uint32_t)(unsigned char)w[k]) {
            k++;
        }
        if (k == n && !w[k]) {
            return true;
        }
    }
    return false;
}

/* str.islower / str.isupper of a whole string */
static bool str_islower(const uint32_t *s, int n) {
    bool cased = false;
    for (int i = 0; i < n; i++) {
        if (pdf_u_upper(s[i]) || pdf_u_title(s[i])) {
            return false;
        }
        cased = cased || pdf_u_lower(s[i]);
    }
    return cased;
}

static bool str_isupper(const uint32_t *s, int n) {
    bool cased = false;
    for (int i = 0; i < n; i++) {
        if (pdf_u_lower(s[i]) || pdf_u_title(s[i])) {
            return false;
        }
        cased = cased || pdf_u_upper(s[i]);
    }
    return cased;
}

static bool is_camel(const uint32_t *t, int n) {
    if (n < 3 || str_isupper(t, n) || str_islower(t, n)) {
        return false;
    }
    /* [A-Z]{2,}s: PDFs, APIs, IDs */
    if (t[n - 1] == 's') {
        bool caps = true;
        for (int i = 0; i < n - 1; i++) {
            caps = caps && t[i] >= 'A' && t[i] <= 'Z';
        }
        if (caps) {
            return false;
        }
    }
    for (int i = 0; i + 1 < n; i++) {
        uint32_t a = t[i];
        uint32_t b = t[i + 1];
        if (pdf_u_lower(a) && pdf_u_upper(b)) {
            return true;
        }
        if (i >= 1 && pdf_u_upper(t[i - 1]) && pdf_u_upper(a) && pdf_u_lower(b)) {
            return true;
        }
    }
    return false;
}

/* The extension after the last '.' of a segment, when it is a code extension. */
static bool has_code_ext(const uint32_t *s, int n) {
    int dot = -1;
    for (int i = 0; i < n; i++) {
        if (s[i] == '.') {
            dot = i;
        }
    }
    return dot >= 0 && in_list_lower(s + dot + 1, n - dot - 1, CODE_EXT);
}

static void path_candidate(const uint32_t *t, int a, int b, candv_t *cv) {
    /* s.strip("./~"), split on '/', drop empties */
    int sa = a;
    int sb = b;
    while (sa < sb && (t[sa] == '.' || t[sa] == '/' || t[sa] == '~')) {
        sa++;
    }
    while (sb > sa && (t[sb - 1] == '.' || t[sb - 1] == '/' || t[sb - 1] == '~')) {
        sb--;
    }
    bool any_seg = false;
    bool all_numeric = true;
    int last_s = -1;
    int last_e = -1;
    for (int i = sa; i <= sb;) {
        int j = i;
        while (j < sb && t[j] != '/') {
            j++;
        }
        if (j > i) {
            any_seg = true;
            for (int k = i; k < j; k++) {
                if (!((t[k] >= '0' && t[k] <= '9') || t[k] == '.' || t[k] == '-')) {
                    all_numeric = false;
                }
            }
            last_s = i;
            last_e = j;
        }
        i = j + 1;
    }
    if (!any_seg || all_numeric) {
        return; /* numeric-path */
    }
    if (in_list_lower(t + a, b - a, SLASH_STOP)) {
        return;
    }
    int slashes = 0;
    for (int i = a; i < b; i++) {
        slashes += t[i] == '/';
    }
    bool has_ext = has_code_ext(t + last_s, last_e - last_s);
    bool prefixed =
        lit_at(t, b, a, "./") || lit_at(t, b, a, "../") || lit_at(t, b, a, "~/") || t[a] == '/';
    cand_t c = {a, b, (slashes >= 2 || has_ext || prefixed) ? MK_PATH : MK_PATH1, 0, 0};
    /* norm: leading ../ ./ ~/ / removed (repeatedly), trailing '/' removed */
    int na = a;
    for (;;) {
        if (lit_at(t, b, na, "../")) {
            na += 3;
        } else if (lit_at(t, b, na, "./") || lit_at(t, b, na, "~/")) {
            na += 2;
        } else if (na < b && t[na] == '/') {
            na += 1;
        } else {
            break;
        }
    }
    int nb = b;
    while (nb > na && t[nb - 1] == '/') {
        nb--;
    }
    c.norm_start = cv->pool.n;
    for (int i = na; i < nb; i++) {
        u32_push(&cv->pool, t[i]);
    }
    c.norm_len = nb - na;
    cand_push(cv, c);
}

static void qual_candidate(const uint32_t *t, int a, int b, candv_t *cv) {
    int ce = b;
    if (b - a >= 2 && t[b - 2] == '(' && t[b - 1] == ')') {
        ce = b - 2;
    }
    bool colons = false;
    bool arrow = false;
    int ns = cv->pool.n;
    for (int i = a; i < ce; i++) {
        if (i + 1 < ce && t[i] == ':' && t[i + 1] == ':') {
            colons = true;
            u32_push(&cv->pool, '.');
            i++;
        } else if (i + 1 < ce && t[i] == '-' && t[i + 1] == '>') {
            arrow = true;
            u32_push(&cv->pool, '.');
            i++;
        } else if (t[i] == '#') {
            u32_push(&cv->pool, '.');
        } else {
            u32_push(&cv->pool, t[i]);
        }
    }
    if (cv->pool.fail) {
        cv->fail = true;
        return;
    }
    const uint32_t *norm = cv->pool.v + ns;
    int nl = cv->pool.n - ns;
    bool short_segs = true;
    bool has_us = false;
    int seg_s = 0;
    int last_s = 0;
    for (int i = 0; i <= nl; i++) {
        if (i == nl || norm[i] == '.') {
            if (i - seg_s > 2) {
                short_segs = false;
            }
            last_s = seg_s;
            seg_s = i + 1;
        } else if (norm[i] == '_') {
            has_us = true;
        }
    }
    bool lower = str_islower(norm, nl);
    if ((short_segs && lower) ||
        (in_list_lower(norm + last_s, nl - last_s, TLDS) && lower && !has_us)) {
        cv->pool.n = ns; /* abbreviation or domain: taken, no candidate */
        return;
    }
    bool file = in_list_lower(norm + last_s, nl - last_s, CODE_EXT) && !colons && !arrow;
    cand_t c = {a, b, file ? MK_FILE : MK_QUAL, ns, nl};
    cand_push(cv, c);
}

static void tok_candidate(const uint32_t *t, int n, int a, int b, uint8_t *taken, candv_t *cv) {
    int len = b - a;
    /* "_" in t.strip("_") */
    int sa = a;
    int sb = b;
    while (sa < sb && t[sa] == '_') {
        sa++;
    }
    while (sb > sa && t[sb - 1] == '_') {
        sb--;
    }
    bool inner_us = false;
    for (int i = sa; i < sb; i++) {
        inner_us = inner_us || t[i] == '_';
    }
    bool dunder = len > 4 && t[a] == '_' && t[a + 1] == '_' && t[b - 1] == '_' && t[b - 2] == '_';
    mention_kind_t kind;
    if (inner_us || dunder) {
        kind = MK_SNAKE;
    } else if (is_camel(t + a, len)) {
        kind = MK_CAMEL;
    } else if (b < n && t[b] == '(' && len >= 2 && !str_isupper(t + a, len)) {
        kind = MK_CALL;
    } else {
        return;
    }
    span_take(taken, a, b);
    cand_t c = {a, b, kind, cv->pool.n, len};
    for (int i = a; i < b; i++) {
        u32_push(&cv->pool, t[i]);
    }
    cand_push(cv, c);
}

static int cand_cmp(const void *x, const void *y) {
    const cand_t *a = (const cand_t *)x;
    const cand_t *b = (const cand_t *)y;
    return a->start < b->start ? -1 : a->start > b->start ? 1 : 0;
}

/* candidates(): line-local candidates of text, sorted by start. */
static bool candidates(const uint32_t *t, int n, bool with_tok, candv_t *cv) {
    uint8_t *taken = (uint8_t *)cbm_calloc(CBM_MEM_CLASS_EXTRACT, (size_t)(n ? n : 1));
    if (!taken) {
        return false;
    }
    url_cache_t uc = {-1, -1, -1, -1};
    for (int p = 0; p < n;) {
        int e = url_at(t, n, p, &uc);
        if (e > p) {
            span_take(taken, p, e);
            p = e;
        } else {
            p++;
        }
    }
    for (int p = 0; p < n;) {
        int e = path_at(t, n, p);
        if (e <= p) {
            p++;
            continue;
        }
        if (span_free(taken, p, e)) {
            span_take(taken, p, e);
            path_candidate(t, p, e, cv);
        }
        p = e;
    }
    for (int p = 0; p < n;) {
        int e = qual_at(t, n, p);
        if (e <= p) {
            p++;
            continue;
        }
        if (span_free(taken, p, e)) {
            span_take(taken, p, e);
            qual_candidate(t, p, e, cv);
        }
        p = e;
    }
    if (with_tok) {
        for (int p = 0; p < n;) {
            int e = tok_at(t, n, p);
            if (e <= p) {
                p++;
                continue;
            }
            if (span_free(taken, p, e)) {
                tok_candidate(t, n, p, e, taken, cv);
            }
            p = e;
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, taken);
    if (cv->n > 1) {
        qsort(cv->v, (size_t)cv->n, sizeof(cand_t), cand_cmp);
    }
    return !cv->fail && !cv->pool.fail;
}

/* ── Joins across a line break ───────────────────────────────────── */

typedef enum { JK_NONE = 0, JK_SOFT, JK_HYPHEN, JK_CODE } join_kind_t;

static join_kind_t join_kind(const uint32_t *prev, int pn, const uint32_t *nxt, int nn) {
    if (!pn || !nn) {
        return JK_NONE;
    }
    if (prev[pn - 1] == 0x00AD && pdf_u_alpha(nxt[0])) {
        return JK_SOFT;
    }
    if (prev[pn - 1] == '-' && pn >= 2 && pdf_u_alpha(prev[pn - 2]) && pdf_u_lower(nxt[0])) {
        return JK_HYPHEN;
    }
    static const char *const sufs[] = {"::", "->", "_", "/"};
    for (int k = 0; k < 4; k++) {
        int sl = (int)strlen(sufs[k]);
        if (pn > sl && lit_at(prev, pn, pn - sl, sufs[k]) && idc(prev[pn - sl - 1]) &&
            idc(nxt[0])) {
            return JK_CODE;
        }
    }
    if (idc(prev[pn - 1])) {
        /* (?:\.|::|->|_)[A-Za-z_] | \(\) at the start of nxt */
        int s = 0;
        if (nxt[0] == '.' || nxt[0] == '_') {
            s = 1;
        } else if (nn >= 2 &&
                   ((nxt[0] == ':' && nxt[1] == ':') || (nxt[0] == '-' && nxt[1] == '>'))) {
            s = 2;
        }
        if (s && s < nn && (is_alpha_ascii(nxt[s]) || nxt[s] == '_')) {
            return JK_CODE;
        }
        if (nn >= 2 && nxt[0] == '(' && nxt[1] == ')') {
            return JK_CODE;
        }
    }
    return JK_NONE;
}

/* [A-Za-z0-9_$@.:/\-#>­] and [A-Za-z0-9_$@.:/\-#>()] */
static bool left_run_char(uint32_t c) {
    return idc(c) || c == '@' || c == '.' || c == ':' || c == '/' || c == '-' || c == '#' ||
           c == '>' || c == 0x00AD;
}

static bool right_run_char(uint32_t c) {
    return idc(c) || c == '@' || c == '.' || c == ':' || c == '/' || c == '-' || c == '#' ||
           c == '>' || c == '(' || c == ')';
}

/* ── Emission ────────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    const char *page_qn;
    uint32_t page;
    const uint32_t *flat;
    int n;
    const int *line_starts;
    int nlines;
    bool failed;
} pdf_emit_t;

static uint32_t line_of(const pdf_emit_t *e, int off) {
    int lo = 0;
    int hi = e->nlines - 1;
    while (lo < hi) {
        int mid = lo + (hi - lo + 1) / 2;
        if (e->line_starts[mid] <= off) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    return (uint32_t)lo + 1;
}

/* R3: "foo.bar=" right after "(" or "," -- a keyword argument, not a reference */
static bool kwarg_at(const pdf_emit_t *e, int start, int end) {
    if (end >= e->n || e->flat[end] != '=' || (end + 1 < e->n && e->flat[end + 1] == '=')) {
        return false;
    }
    int lim = start - PDF_R3_WINDOW < 0 ? 0 : start - PDF_R3_WINDOW;
    for (int i = start - 1; i >= lim; i--) {
        if (!pdf_u_space(e->flat[i])) {
            return e->flat[i] == '(' || e->flat[i] == ',';
        }
    }
    return false;
}

static int u8_put(uint32_t cp, char *o) {
    if (cp < 0x80) {
        o[0] = (char)cp;
        return 1;
    }
    if (cp < 0x800) {
        o[0] = (char)(0xC0 | (cp >> 6));
        o[1] = (char)(0x80 | (cp & 0x3F));
        return 2;
    }
    if (cp < 0x10000) {
        o[0] = (char)(0xE0 | (cp >> 12));
        o[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        o[2] = (char)(0x80 | (cp & 0x3F));
        return 3;
    }
    o[0] = (char)(0xF0 | (cp >> 18));
    o[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    o[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
    o[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

static int kind_syntax(mention_kind_t k) {
    switch (k) {
    case MK_PATH:
    case MK_PATH1:
        return CBM_DOCLINK_PDF_PATH;
    case MK_FILE:
        return CBM_DOCLINK_PDF_FILE;
    case MK_QUAL:
        return CBM_DOCLINK_PDF_QN;
    default:
        return CBM_DOCLINK_PDF_NAME;
    }
}

static bool structural(mention_kind_t k) {
    return k == MK_PATH || k == MK_PATH1 || k == MK_FILE || k == MK_QUAL;
}

/* Push a token; returns its index in the file's token array, or -1. */
static int emit(pdf_emit_t *e, int syntax, const char *raw, uint32_t line, uint16_t flags) {
    CBMArena *a = e->ctx->arena;
    char *r = cbm_arena_strdup(a, raw);
    if (!r) {
        e->failed = true;
        return -1;
    }
    CBMDocLink link = {
        .source_qn = e->page_qn,
        .raw = r,
        .line = line,
        .def_line = e->page,
        .syntax = (uint16_t)syntax,
        .flags = flags,
    };
    int idx = e->ctx->result->doc_links.count;
    cbm_doclinks_push(&e->ctx->result->doc_links, a, link);
    if (e->ctx->result->doc_links.count != idx + 1) {
        e->failed = true;
        return -1;
    }
    return idx;
}

typedef struct {
    char *p;
    size_t n;
    size_t cap;
    bool fail;
} sbuf_t;

static void sb_put(sbuf_t *b, const char *s, size_t n) {
    if (b->fail) {
        return;
    }
    if (b->n + n + 1 > b->cap) {
        size_t ncap = b->cap ? b->cap * 2 : 256;
        while (ncap < b->n + n + 1) {
            ncap *= 2;
        }
        char *g = (char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, b->p, ncap);
        if (!g) {
            b->fail = true;
            return;
        }
        b->p = g;
        b->cap = ncap;
    }
    memcpy(b->p + b->n, s, n);
    b->n += n;
    b->p[b->n] = '\0';
}

static void sb_cps(sbuf_t *b, const uint32_t *s, int n) {
    char u[PDF_U8_MAX];
    for (int i = 0; i < n; i++) {
        int k = u8_put(s[i], u);
        sb_put(b, u, (size_t)k);
    }
}

/* One page: its line-local structural tokens, then its join proposals. */
static void scan_page(CBMExtractCtx *ctx, const char *text, size_t len, uint32_t page,
                      const char *page_qn) {
    u32v_t norm = {0};
    if (!normalise(text, len, &norm)) {
        u32_free(&norm);
        ctx->result->doc_links.failed = true;
        return;
    }
    /* lines, each stripped; flat = lines joined by '\n' */
    u32v_t flat = {0};
    int *starts = NULL;
    int nlines = 0;
    int cap = 0;
    int *lens = NULL;
    for (int i = 0; i <= norm.n;) {
        int j = i;
        while (j < norm.n && norm.v[j] != '\n') {
            j++;
        }
        int a = i;
        int b = j;
        while (a < b && pdf_u_space(norm.v[a])) {
            a++;
        }
        while (b > a && pdf_u_space(norm.v[b - 1])) {
            b--;
        }
        if (nlines == cap) {
            cap = cap ? cap * 2 : PDF_LIST_MIN;
            int *g1 = (int *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, starts, (size_t)cap * sizeof(int));
            if (g1) {
                starts = g1;
            }
            int *g2 = (int *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, lens, (size_t)cap * sizeof(int));
            if (g2) {
                lens = g2;
            }
            if (!g1 || !g2) {
                flat.fail = true;
                break;
            }
        }
        if (nlines) {
            u32_push(&flat, '\n');
        }
        starts[nlines] = flat.n;
        lens[nlines] = b - a;
        nlines++;
        for (int k = a; k < b; k++) {
            u32_push(&flat, norm.v[k]);
        }
        if (j >= norm.n) {
            break;
        }
        i = j + 1;
    }
    u32_free(&norm);
    candv_t cv = {0};
    if (flat.fail || !candidates(flat.v, flat.n, false, &cv)) {
        ctx->result->doc_links.failed = true;
        goto done;
    }
    pdf_emit_t e = {ctx, page_qn, page, flat.v, flat.n, starts, nlines, false};
    /* the token index of every structural candidate (-1: not emitted) */
    int *tok = (int *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(cv.n ? cv.n : 1) * sizeof(int));
    if (!tok) {
        ctx->result->doc_links.failed = true;
        goto done;
    }
    sbuf_t sb = {0};
    for (int i = 0; i < cv.n; i++) {
        const cand_t *c = &cv.v[i];
        tok[i] = -1;
        if (!structural(c->kind) || kwarg_at(&e, c->start, c->end)) {
            continue;
        }
        sb.n = 0;
        sb_cps(&sb, cv.pool.v + c->norm_start, c->norm_len);
        if (sb.fail) {
            break;
        }
        tok[i] = emit(&e, kind_syntax(c->kind), sb.p ? sb.p : "", line_of(&e, c->start), 0);
    }
    /* The emitted candidates of each line, ascending (CSR: lc[lc_off[l] ..
     * lc_off[l + 1])). A join's span covers the end of one line and the start of
     * the next, so only their candidates can overlap it; reading every candidate
     * of the page per join cost (joins x candidates), quadratic in the text. */
    int *lc_off = (int *)cbm_calloc(CBM_MEM_CLASS_EXTRACT, (size_t)(nlines + 2) * sizeof(int));
    int *lc = NULL;
    if (!lc_off) {
        e.failed = true;
    } else {
        for (int f = 0; f < cv.n; f++) {
            if (tok[f] >= 0) {
                int end = cv.v[f].end > cv.v[f].start ? cv.v[f].end - 1 : cv.v[f].start;
                for (uint32_t l = line_of(&e, cv.v[f].start); l <= line_of(&e, end); l++) {
                    lc_off[l]++; /* line_of is 1-based: line l - 1 counts at l */
                }
            }
        }
        for (int l = 0; l < nlines; l++) {
            lc_off[l + 1] += lc_off[l];
        }
        lc = (int *)cbm_alloc(CBM_MEM_CLASS_EXTRACT,
                              (size_t)(lc_off[nlines] ? lc_off[nlines] : 1) * sizeof(int));
        if (!lc) {
            e.failed = true;
        } else {
            /* lc_off[l] runs as line l's cursor, then shifts back to its start */
            for (int f = 0; f < cv.n; f++) {
                if (tok[f] >= 0) {
                    int end = cv.v[f].end > cv.v[f].start ? cv.v[f].end - 1 : cv.v[f].start;
                    for (uint32_t l = line_of(&e, cv.v[f].start); l <= line_of(&e, end); l++) {
                        lc[lc_off[l - 1]++] = f;
                    }
                }
            }
            for (int l = nlines; l > 0; l--) {
                lc_off[l] = lc_off[l - 1];
            }
            lc_off[0] = 0;
        }
    }
    /* joins */
    for (int li = 0; li + 1 < nlines && !sb.fail && !e.failed; li++) {
        const uint32_t *prev = flat.v + starts[li];
        const uint32_t *nxt = flat.v + starts[li + 1];
        int pn = lens[li];
        int nn = lens[li + 1];
        join_kind_t jk = join_kind(prev, pn, nxt, nn);
        if (jk == JK_NONE) {
            continue;
        }
        int ls = pn;
        while (ls > 0 && left_run_char(prev[ls - 1])) {
            ls--;
        }
        int re = 0;
        while (re < nn && right_run_char(nxt[re])) {
            re++;
        }
        if (ls == pn || re == 0) {
            continue;
        }
        int left_len = pn - ls;
        int dropped = (jk == JK_HYPHEN || jk == JK_SOFT) ? 1 : 0;
        int junction = left_len - dropped;
        u32v_t joined = {0};
        for (int k = ls; k < pn - dropped; k++) {
            u32_push(&joined, prev[k]);
        }
        for (int k = 0; k < re; k++) {
            u32_push(&joined, nxt[k]);
        }
        candv_t jc = {0};
        if (joined.fail || !candidates(joined.v, joined.n, true, &jc)) {
            u32_free(&joined);
            candv_free(&jc);
            ctx->result->doc_links.failed = true;
            break;
        }
        int a0 = starts[li] + ls;
        int b0 = starts[li + 1];
        for (int k = 0; k < jc.n; k++) {
            const cand_t *c = &jc.v[k];
            if (!(c->start < junction && junction < c->end)) {
                continue;
            }
            int s_flat = a0 + c->start;
            int e_flat = b0 + (c->end - junction);
            if (structural(c->kind) && kwarg_at(&e, s_flat, e_flat)) {
                continue;
            }
            sb.n = 0;
            const uint32_t *nm = jc.pool.v + c->norm_start;
            sb_cps(&sb, nm, c->norm_len);
            if (dropped) {
                /* alts: the norm with the hyphen kept, at the junction */
                /* Python slices clamp: past the end, the hyphen goes last */
                int at = junction - c->start;
                at = at > c->norm_len ? c->norm_len : at;
                char sep = PDF_JOIN_SEP;
                sb_put(&sb, &sep, 1);
                sb_cps(&sb, nm, at);
                sb_put(&sb, "-", 1);
                sb_cps(&sb, nm + at, c->norm_len - at);
            }
            char fs = PDF_JOIN_FRAG;
            sb_put(&sb, &fs, 1);
            bool first = true;
            int ia = lc_off[li];
            int ea = lc_off[li + 1];
            int ib = lc_off[li + 1];
            int eb = lc_off[li + 2];
            while (ia < ea || ib < eb) {
                int f;
                if (ib >= eb || (ia < ea && lc[ia] < lc[ib])) {
                    f = lc[ia++];
                } else if (ia >= ea || lc[ib] < lc[ia]) {
                    f = lc[ib++];
                } else {
                    f = lc[ia++]; /* on both lines */
                    ib++;
                }
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
                atomic_fetch_add_explicit(&g_join_steps, 1, memory_order_relaxed);
#endif
                const cand_t *x = &cv.v[f];
                if (x->start < e_flat && x->end > s_flat) {
                    char num[16];
                    int nlen = snprintf(num, sizeof(num), first ? "%d" : ",%d", tok[f]);
                    sb_put(&sb, num, (size_t)nlen);
                    first = false;
                }
            }
            if (sb.fail) {
                break;
            }
            emit(&e, kind_syntax(c->kind), sb.p, line_of(&e, s_flat), CBM_DOCLINK_FLAG_JOIN);
        }
        u32_free(&joined);
        candv_free(&jc);
    }
    if (sb.fail || e.failed) {
        ctx->result->doc_links.failed = true;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, sb.p);
    cbm_free(CBM_MEM_CLASS_EXTRACT, tok);
    cbm_free(CBM_MEM_CLASS_EXTRACT, lc_off);
    cbm_free(CBM_MEM_CLASS_EXTRACT, lc);
done:
    candv_free(&cv);
    u32_free(&flat);
    cbm_free(CBM_MEM_CLASS_EXTRACT, starts);
    cbm_free(CBM_MEM_CLASS_EXTRACT, lens);
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
void cbm_pdf_test_scan_page(CBMExtractCtx *ctx, const char *text, size_t len, uint32_t page,
                            const char *page_qn) {
    scan_page(ctx, text, len, page, page_qn);
}

size_t cbm_pdf_test_join_steps(void) {
    return atomic_load_explicit(&g_join_steps, memory_order_relaxed);
}
#endif

/* ── The document ────────────────────────────────────────────────── */

/* R1: the field test's category(): a PDF under a test or fixture directory. */
static bool fixture_pdf(const char *rel_path) {
    const char *s = rel_path;
    for (;;) {
        const char *slash = strchr(s, '/');
        if (!slash) {
            return false;
        }
        size_t n = (size_t)(slash - s);
        char comp[256];
        if (n < sizeof(comp)) {
            for (size_t i = 0; i < n; i++) {
                comp[i] = (char)(s[i] >= 'A' && s[i] <= 'Z' ? s[i] + ('a' - 'A') : s[i]);
            }
            comp[n] = '\0';
            for (int i = 0; FIXTURE_DIRS[i]; i++) {
                if (strcmp(comp, FIXTURE_DIRS[i]) == 0) {
                    return true;
                }
            }
            if (strstr(comp, "test") && strcmp(comp, "testing") != 0) {
                return true;
            }
        }
        s = slash + 1;
    }
}

/* A page's docstring (its snippet): the page text, up to PDF_PAGE_DOC_MAX bytes
 * cut on a character boundary and then said so. A page holds a few kilobytes;
 * only a crafted text layer reaches the bound, and the mentions are still read
 * from the whole text. */
static const char *page_doc(CBMArena *a, const cbm_pdf_page_t *pg) {
    if (pg->len <= PDF_PAGE_DOC_MAX) {
        return pg->text;
    }
    size_t cut = PDF_PAGE_DOC_MAX;
    while (cut > 0 && ((unsigned char)pg->text[cut] & 0xC0U) == 0x80U) {
        cut--; /* not inside a UTF-8 sequence */
    }
    const char *doc = cbm_arena_sprintf(a, "%.*s\n[page text cut at %zu of %zu bytes]", (int)cut,
                                        pg->text, cut, pg->len);
    return doc ? doc : pg->text;
}

void cbm_pdf_extract_document(CBMExtractCtx *ctx) {
    CBMFileResult *result = ctx->result;
    CBMArena *a = ctx->arena;
    cbm_pdf_result_t pr;
    if (!cbm_pdf_extract((const unsigned char *)ctx->source, (size_t)ctx->source_len, a, &pr)) {
        result->has_error = true;
        result->error_msg = cbm_arena_strdup(a, "pdf: out of memory");
        return;
    }
    if (pr.status != CBM_PDF_OK) {
        static const char *const why[] = {"ok", "pdf: not a PDF", "pdf: no pages", "pdf: encrypted",
                                          "pdf: out of memory"};
        result->has_error = pr.status != CBM_PDF_ENCRYPTED;
        result->error_msg = cbm_arena_strdup(a, why[pr.status]);
        return;
    }
    bool fixture = fixture_pdf(ctx->rel_path ? ctx->rel_path : "");
    for (int i = 0; i < pr.npages; i++) {
        uint32_t page = (uint32_t)i + 1;
        char name[PDF_PAGE_NAME];
        char seg[PDF_PAGE_NAME];
        snprintf(name, sizeof(name), "page %u", page);
        snprintf(seg, sizeof(seg), "page_%u", page);
        char *nm = cbm_arena_strdup(a, name);
        char *qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, seg);
        char *pv = cbm_arena_sprintf(a, "%u", page);
        const char **kv = (const char **)cbm_arena_alloc(a, 3 * sizeof(char *));
        if (!nm || !qn || !pv || !kv) {
            result->has_error = true;
            return;
        }
        kv[0] = "page";
        kv[1] = pv;
        kv[2] = NULL;
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = nm;
        def.qualified_name = qn;
        def.label = "Section";
        def.file_path = ctx->rel_path;
        def.start_line = page;
        def.end_line = page;
        def.is_exported = true;
        def.docstring = pr.pages[i].len ? page_doc(a, &pr.pages[i]) : NULL;
        def.extra_props = kv;
        cbm_defs_push(&result->defs, a, def);
        if (!fixture && pr.pages[i].len) {
            scan_page(ctx, pr.pages[i].text, pr.pages[i].len, page, qn);
        }
    }
}
