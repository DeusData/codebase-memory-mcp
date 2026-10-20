/*
 * doclink_md.c — Markdown references to code: the extraction half.
 *
 * In a Markdown document the text IS the documentation, so its references are
 * not taken from a definition's doc comment: one pass over the file's bytes
 * finds the explicit ones and gives each to the section it is written in --
 * the Section definition (a heading) at or above its line, or the file itself
 * for text before the first heading (CBM_DOCLINK_FLAG_FILE). Families
 * (doclink.h):
 *
 *   link       the destination of an inline link `[text](dest)` or of a link
 *              reference definition `[label]: dest`, when it is no URL, no
 *              in-page anchor and no image
 *   path       a bare path in prose: a slash and a file extension
 *   code_path  a code span, or an HTML <code> span, whose text is a path, a
 *              file name or a directory, with an optional line range
 *              (`#L3-L9`, `:3-9`) or member (`path::Name`)
 *
 * Never scanned: YAML front matter, fenced code blocks, HTML comments, the
 * import/export lines of MDX. Never a reference here: a URL, an in-page
 * anchor, an image, a name without a path (a bare name is at most a
 * suggestion, never an edge).
 *
 * Cost. Each scan of a line is linear in the line, or n log n where it sorts.
 * The searches that could otherwise rescan a stretch of the line for every
 * candidate (the end of a link destination, the closing parenthesis, the
 * closing backticks of a code span) are answered from position lists of the
 * few bytes they look for, by binary search. The lists grow with the number
 * of those bytes, not with the line, and are released at the end of the
 * file. A list that cannot grow marks the file's doc links failed, so the
 * resolving half fails the layer instead of publishing a graph that silently
 * misses references.
 *
 * The rules are those the documentation field tests measured (H0): explicit
 * paths and links bind EXACTLY, at 99 % precision on 53 repositories.
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define MD_SLEN(s) (sizeof(s) - SKIP_ONE)

enum {
    MD_MASK = 0x01,         /* a byte the later scans of the line must not see */
    MD_FENCE_MIN = 3,       /* ``` or ~~~ */
    MD_INDENT_MAX = 3,      /* a link reference definition may be indented this far */
    MD_SPAN_MAX = 150,      /* longer code-span text is a snippet, not a name */
    MD_EXT_MAX = 12,        /* a file extension: a letter and at most 11 more */
    MD_TAG_MAX = 200,       /* an HTML tag inside a <code> span is at most this long */
    MD_CALL_ARGS = 60,      /* `name(args)`: the argument text of a call form */
    MD_CALL_ARGS_WIDE = 80, /* the same with spaces or commas in the arguments */
    MD_TINY_DOTFILE = 3,    /* `.a` is no file name, `.ab` is */
    MD_LIST_INIT = 16,      /* first capacity of a position list */
    MD_NUM_DIGITS = 9,      /* a line number has at most this many digits */
    MD_PAIR = 2,            /* ints per pair entry */
    MD_URL_SCHEMES = 3,     /* http, https, ftp */
    MD_BOM_LEN = 3,         /* EF BB BF */
};

/* UTF-8 well-formedness (RFC 3629, table 3-7 of the Unicode standard). */
enum {
    UTF8_CTRL_END = 0x20,
    UTF8_DEL = 0x7f,
    UTF8_ASCII_END = 0x80,
    UTF8_CONT_MIN = 0x80,
    UTF8_CONT_MAX = 0xBF,
    UTF8_LEAD2_MIN = 0xC2,
    UTF8_LEAD2_MAX = 0xDF,
    UTF8_LEAD3_MIN = 0xE0,
    UTF8_LEAD3_MAX = 0xEF,
    UTF8_E0_SECOND_MIN = 0xA0,
    UTF8_SURROGATE_LEAD = 0xED,
    UTF8_ED_SECOND_MAX = 0x9F,
    UTF8_LEAD4_MIN = 0xF0,
    UTF8_LEAD4_MAX = 0xF4,
    UTF8_F0_SECOND_MIN = 0x90,
    UTF8_F4_SECOND_MAX = 0x8F,
};

/* ── Small character classes (ASCII; bytes >= 0x80 are word bytes) ── */

static bool md_blank(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

static bool md_sp_tab(unsigned char c) {
    return c == ' ' || c == '\t';
}

static bool md_alpha(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static bool md_digit(unsigned char c) {
    return c >= '0' && c <= '9';
}

static bool md_alnum(unsigned char c) {
    return md_alpha(c) || md_digit(c);
}

/* Python's \w on text: letters, digits, '_' and any non-ASCII byte. */
static bool md_word(unsigned char c) {
    return md_alnum(c) || c == '_' || c >= 0x80;
}

static unsigned char md_lower(unsigned char c) {
    return (c >= 'A' && c <= 'Z') ? (unsigned char)(c - 'A' + 'a') : c;
}

static bool md_has(const char *s, size_t n, char c) {
    return n > 0 && memchr(s, c, n) != NULL;
}

static bool md_eq_ci(const char *s, size_t n, const char *word) {
    size_t w = strlen(word);
    if (n != w) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (md_lower((unsigned char)s[i]) != (unsigned char)word[i]) {
            return false;
        }
    }
    return true;
}

static bool md_in_list(const char *s, size_t n, const char *const *list, bool ci) {
    for (size_t i = 0; list[i]; i++) {
        if (ci ? md_eq_ci(s, n, list[i]) : (strlen(list[i]) == n && memcmp(s, list[i], n) == 0)) {
            return true;
        }
    }
    return false;
}

/* File extensions that make a dotted token a FILE NAME. Strict: none of them
 * is a common member name (`.log`, `.get` and `.map` are not here). */
static const char *const MD_FILE_EXT[] = {
    "py", "pyi", "js", "jsx", "mjs", "cjs", "ts", "tsx", "mts", "cts", "go", "rs", "c", "h", "cc",
    "cpp", "cxx", "hpp", "hh", "java", "kt", "kts", "scala", "groovy", "cs", "php", "rb", "sh",
    "bash", "zsh", "ps1", "md", "mdx", "rst", "adoc", "txt", "json", "jsonc", "yaml", "yml", "toml",
    "ini", "cfg", "xml", "html", "htm", "css", "scss", "sass", "less", "vue", "svelte", "proto",
    "sql", "lock", "gradle", "csproj", "sln", "fsproj", "props", "targets", "cmake", "mk", "tf",
    "tfvars", "hcl", "png", "jpg", "jpeg", "gif", "svg", "pdf", "ipynb", "dockerfile", "swift",
    "dart", "lua", "pl", "pm", "ex", "exs", "erl", "clj", "hs", "ml", "zig", "sol", "graphql",
    "gql", "bzl", "nix", "plist", "env", "conf", "properties", "editorconfig", "gitignore", "npmrc",
    "nvmrc", "j2", "jinja", "tmpl", "tpl", "erb", "hbs", "mustache", "patch", "diff", "whl", "jar",
    "zip", "gz", "tgz", "so", "dll", "dylib", "wasm", "service", "socket", "timer", "mod", "sum",
    /* built and shipped files, never a member's name (`OliveTin.exe` took the
     * module stem of OliveTin.exe.manifest in a held-out audit) */
    "exe", "msi", "deb", "rpm", "dmg", "tar", "xz", "bz2", "7z", "apk", "ipa", "manifest", "csv",
    "tsv", NULL};

/* File names that are files without an extension. */
static const char *const MD_SPECIAL_FILES[] = {
    "Makefile", "Dockerfile", "Containerfile", "Jenkinsfile", "Vagrantfile", "Gemfile", "Rakefile",
    "Procfile", "Kconfig",    "CODEOWNERS",    "MAINTAINERS", "LICENSE",     "NOTICE",  "go.mod",
    "go.sum",   "Cargo.lock", "Justfile",      "BUILD",       "WORKSPACE",   NULL};

/* Host names written as code: a site, not a file. */
static const char *const MD_DOMAINS[] = {"docs.rs",           "crates.io",       "pkg.go.dev",
                                         "npmjs.com",         "pypi.org",        "github.com",
                                         "gitlab.com",        "readthedocs.io",  "rubygems.org",
                                         "nuget.org",         "go.dev",          "golang.org",
                                         "godoc.org",         "packagist.org",   "hex.pm",
                                         "mvnrepository.com", "docs.python.org", NULL};

/* `.ext`, a letter and up to 11 of [A-Za-z0-9_+-], at the end of `s`. */
static bool md_ext_ok(const char *s, size_t n) {
    const char *dot = NULL;
    for (size_t i = n; i > 0; i--) {
        if (s[i - SKIP_ONE] == '.') {
            dot = s + i - SKIP_ONE;
            break;
        }
    }
    if (!dot) {
        return false;
    }
    size_t ext_len = n - (size_t)(dot - s) - SKIP_ONE;
    if (ext_len == 0 || ext_len > MD_EXT_MAX || !md_alpha((unsigned char)dot[SKIP_ONE])) {
        return false;
    }
    for (size_t i = 2; i <= ext_len; i++) {
        unsigned char c = (unsigned char)dot[i];
        if (!md_alnum(c) && c != '_' && c != '+' && c != '-') {
            return false;
        }
    }
    return true;
}

/* `^[A-Za-z][A-Za-z0-9+.-]*://`, `mailto:` or `www.`. */
static bool md_is_url(const char *s, size_t n) {
    if (n >= MD_SLEN("mailto:") && md_eq_ci(s, MD_SLEN("mailto:"), "mailto:")) {
        return true;
    }
    if (n >= MD_SLEN("www.") && md_eq_ci(s, MD_SLEN("www."), "www.")) {
        return true;
    }
    if (n == 0 || !md_alpha((unsigned char)s[0])) {
        return false;
    }
    size_t i = SKIP_ONE;
    while (i < n && (md_alnum((unsigned char)s[i]) || s[i] == '+' || s[i] == '.' || s[i] == '-')) {
        i++;
    }
    return i + MD_SLEN("://") <= n && memcmp(s + i, "://", MD_SLEN("://")) == 0;
}

/* `^[A-Za-z_$][\w$]*$` */
static bool md_ident(const char *s, size_t n) {
    if (n == 0) {
        return false;
    }
    unsigned char c0 = (unsigned char)s[0];
    if (!(md_alpha(c0) || c0 == '_' || c0 == '$' || c0 >= 0x80)) {
        return false;
    }
    for (size_t i = SKIP_ONE; i < n; i++) {
        if (!md_word((unsigned char)s[i]) && s[i] != '$') {
            return false;
        }
    }
    return true;
}

/* Every byte of s[0..n) is a word byte or one of `extra`. */
static bool md_all_of(const char *s, size_t n, const char *extra) {
    if (n == 0) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!md_word((unsigned char)s[i]) && !strchr(extra, s[i])) {
            return false;
        }
    }
    return true;
}

/* ── Line numbers written with a path ────────────────────────────── */

/* The decimal number s[0..n): false when it is empty, not all digits or too
 * long to be a line. */
static bool md_number(const char *s, size_t n, uint32_t *out) {
    if (n == 0 || n > MD_NUM_DIGITS) {
        return false;
    }
    uint32_t v = 0;
    for (size_t i = 0; i < n; i++) {
        if (!md_digit((unsigned char)s[i])) {
            return false;
        }
        v = (v * CBM_DECIMAL_BASE) + (uint32_t)(s[i] - '0');
    }
    *out = v;
    return v > 0;
}

/* Digits ending at s[end): their start. */
static size_t md_digits_back(const char *s, size_t end) {
    size_t i = end;
    while (i > 0 && md_digit((unsigned char)s[i - SKIP_ONE])) {
        i--;
    }
    return i;
}

bool cbm_doclink_md_line_fragment(const char *frag, uint32_t *first, uint32_t *last) {
    /* L<a>[C<c>][-[L]<b>[C<c>]] */
    if (!frag || frag[0] != 'L') {
        return false;
    }
    const char *p = frag + SKIP_ONE;
    const char *q = p;
    while (md_digit((unsigned char)*q)) {
        q++;
    }
    uint32_t a = 0;
    if (!md_number(p, (size_t)(q - p), &a)) {
        return false;
    }
    if (*q == 'C') {
        q++;
        while (md_digit((unsigned char)*q)) {
            q++;
        }
    }
    uint32_t b = a;
    if (*q == '-') {
        q++;
        if (*q == 'L') {
            q++;
        }
        p = q;
        while (md_digit((unsigned char)*q)) {
            q++;
        }
        if (!md_number(p, (size_t)(q - p), &b)) {
            return false;
        }
        if (*q == 'C') {
            q++;
            while (md_digit((unsigned char)*q)) {
                q++;
            }
        }
    }
    if (*q != '\0') {
        return false;
    }
    *first = a;
    *last = b < a ? a : b;
    return true;
}

/* A line range at the end of p[0..*n): `#L<a>[-[L]<b>]`, `:<a>`, `:<a>-<b>`
 * or `:<a>:<b>`. On a match the range is cut off (*n shrinks). */
static bool md_line_suffix(const char *p, size_t *n, uint32_t *first, uint32_t *last) {
    size_t len = *n;
    /* `#L` form: the last '#' */
    for (size_t i = len; i > 0; i--) {
        if (p[i - SKIP_ONE] != '#') {
            continue;
        }
        size_t h = i - SKIP_ONE;
        if (h + SKIP_ONE < len && p[h + SKIP_ONE] == 'L') {
            size_t a0 = h + PAIR_LEN;
            size_t a1 = a0;
            while (a1 < len && md_digit((unsigned char)p[a1])) {
                a1++;
            }
            uint32_t a = 0;
            if (md_number(p + a0, a1 - a0, &a)) {
                uint32_t b = a;
                size_t k = a1;
                bool ok = k == len;
                if (!ok && p[k] == '-') {
                    k++;
                    if (k < len && p[k] == 'L') {
                        k++;
                    }
                    ok = md_number(p + k, len - k, &b);
                }
                if (ok) {
                    *first = a;
                    *last = b < a ? a : b;
                    *n = h;
                    return true;
                }
            }
        }
        break;
    }
    /* `:` form: digits at the end */
    size_t d2 = md_digits_back(p, len);
    uint32_t b = 0;
    if (d2 == len || d2 == 0 || !md_number(p + d2, len - d2, &b)) {
        return false;
    }
    char sep = p[d2 - SKIP_ONE];
    if (sep == '-' || sep == ':') {
        size_t d1 = md_digits_back(p, d2 - SKIP_ONE);
        uint32_t a = 0;
        if (d1 > 0 && d1 < d2 - SKIP_ONE && p[d1 - SKIP_ONE] == ':' &&
            md_number(p + d1, d2 - SKIP_ONE - d1, &a)) {
            *first = a;
            *last = b < a ? a : b;
            *n = d1 - SKIP_ONE;
            return true;
        }
    }
    if (sep == ':') {
        *first = b;
        *last = b;
        *n = d2 - SKIP_ONE;
        return true;
    }
    return false;
}

/* ── Code-span classification ────────────────────────────────────── */

/* Drop one trailing balanced `<...>` or `[...]` (type arguments). */
static size_t md_strip_generics(const char *s, size_t n) {
    static const char pairs[][PAIR_LEN] = {{'<', '>'}, {'[', ']'}};
    for (size_t p = 0; p < sizeof(pairs) / sizeof(pairs[0]); p++) {
        char o = pairs[p][0];
        char c = pairs[p][SKIP_ONE];
        if (n == 0 || s[n - SKIP_ONE] != c || !md_has(s, n, o)) {
            continue;
        }
        int depth = 0;
        for (size_t k = n; k > 0; k--) {
            char ch = s[k - SKIP_ONE];
            if (ch == c) {
                depth++;
            } else if (ch == o) {
                depth--;
                if (depth == 0) {
                    if (k - SKIP_ONE > 0) {
                        return k - SKIP_ONE;
                    }
                    break;
                }
            }
        }
        return n;
    }
    return n;
}

/* `name(args)` with a short argument list that holds no parenthesis: the
 * callee's length, or 0. `wide` is the first form of the field test (args
 * with a space or comma, up to 80 bytes, callee [\w$.:#\\]); otherwise the
 * second (args up to 60 bytes, callee [\w$.:#\\<>\[\]!~&*-]). */
static size_t md_callee(const char *s, size_t n, bool wide) {
    if (n < PAIR_LEN || s[n - SKIP_ONE] != ')') {
        return 0;
    }
    size_t open = n;
    for (size_t i = n - SKIP_ONE; i > 0; i--) {
        char c = s[i - SKIP_ONE];
        if (c == ')') {
            return 0;
        }
        if (c == '(') {
            open = i - SKIP_ONE;
            break;
        }
    }
    if (open == n || open == 0) {
        return 0;
    }
    size_t args = n - open - PAIR_LEN;
    if (args > (wide ? (size_t)MD_CALL_ARGS_WIDE : (size_t)MD_CALL_ARGS)) {
        return 0;
    }
    if (wide && !md_has(s + open + SKIP_ONE, args, ' ') &&
        !md_has(s + open + SKIP_ONE, args, ',')) {
        return 0;
    }
    return md_all_of(s, open, wide ? "$.:#\\" : "$.:#\\<>[]!~&*-") ? open : 0;
}

static void md_shape(CBMDocLinkMdPath *out, CBMDocLinkMdShape shape, const char *path,
                     const char *member, uint32_t first, uint32_t last) {
    out->shape = shape;
    out->path = path;
    out->member = member;
    out->first_line = first;
    out->last_line = last;
    out->instance = false;
    out->colon = false;
}

/* The path branch: `s` has a slash (or a Windows path with an extension). */
static bool md_classify_path(char *s, size_t n, CBMDocLinkMdPath *out) {
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '\\') {
            s[i] = '/';
        }
    }
    if (n >= PAIR_LEN && s[0] == '/' && s[SKIP_ONE] == '/') {
        return false;
    }
    uint32_t first = 0;
    uint32_t last = 0;
    (void)md_line_suffix(s, &n, &first, &last);
    s[n] = '\0';
    const char *member = NULL;
    char *sep = strstr(s, "::");
    if (sep) {
        *sep = '\0';
        member = sep + PAIR_LEN;
        n = (size_t)(sep - s);
        if (!member[0]) {
            member = NULL;
        }
    }
    if (n == 0) {
        return false;
    }
    bool named = false;
    for (size_t i = 0; i < n && !named; i++) {
        named = md_word((unsigned char)s[i]);
    }
    if (!named) {
        return false; /* `../`, `./`, `/`: a traversal or a root, naming no directory */
    }
    /* an import path (`github.com/x/y`, `k8s.io/client-go`): a host-like first
     * segment -- dot-separated word groups -- and a last segment (as written,
     * a trailing slash leaves it empty) without a file extension */
    size_t host = 0;
    while (host < n && s[host] != '/') {
        host++;
    }
    bool hostlike = host > 0 && host < n && md_has(s, host, '.') && md_all_of(s, host, ".-") &&
                    s[0] != '.' && s[host - SKIP_ONE] != '.';
    for (size_t i = 0; hostlike && i + SKIP_ONE < host; i++) {
        hostlike = !(s[i] == '.' && s[i + SKIP_ONE] == '.');
    }
    size_t tail = n;
    while (tail > 0 && s[tail - SKIP_ONE] != '/') {
        tail--;
    }
    if (hostlike && !md_ext_ok(s + tail, n - tail)) {
        return false;
    }
    /* the base name, after any trailing slashes */
    size_t end = n;
    while (end > 0 && s[end - SKIP_ONE] == '/') {
        end--;
    }
    size_t base = end;
    while (base > 0 && s[base - SKIP_ONE] != '/') {
        base--;
    }
    const char *last_seg = s + base;
    size_t last_len = end - base;
    if (!md_all_of(s, n, "./@+~-")) {
        return false;
    }
    if (s[n - SKIP_ONE] == '/') {
        md_shape(out, CBM_DOCLINK_MD_DIR, s, NULL, 0, 0);
        return true;
    }
    const char *dot = last_len > 0 ? memchr(last_seg, '.', last_len) : NULL;
    if (dot && last_seg[0] != '.') {
        const char *ext = strrchr(last_seg, '.') + SKIP_ONE;
        size_t ext_len = (size_t)(last_seg + last_len - ext);
        if (ext_len > 0 && ext[0] >= 'A' && ext[0] <= 'Z' && md_ident(ext, ext_len) &&
            !md_in_list(ext, ext_len, MD_FILE_EXT, true)) {
            return false; /* `pkg/Type.Member`: a name, not a file (a later rung) */
        }
        if (md_ext_ok(last_seg, last_len)) {
            md_shape(out, CBM_DOCLINK_MD_FILE, s, member, first, last);
            return true;
        }
    }
    if (last_len > SKIP_ONE && last_seg[0] == '.') {
        md_shape(out, CBM_DOCLINK_MD_FILE, s, NULL, first, last);
        return true;
    }
    md_shape(out, CBM_DOCLINK_MD_DIR, s, NULL, 0, 0);
    return true;
}

static const char *const MD_RUST_ROOTS[] = {"crate", "self", "super", "Self", NULL};
static const char *const MD_INSTANCE_ROOTS[] = {"self", "this", "cls", "$this", NULL};

/* Split s[0..n) at every `sep`, check that each piece is an identifier (type
 * arguments dropped), and join the pieces with '.' in place. A first piece in
 * `skip_first` (Rust's `crate`, an instance's `self`) is dropped and *skipped
 * says so. False when a piece is no identifier or fewer than two remain. */
static bool md_join(char *s, size_t n, const char *sep, const char *const *skip_first,
                    bool *skipped, CBMDocLinkMdPath *out) {
    size_t sl = strlen(sep);
    size_t pieces = 0;
    size_t w = 0;
    size_t i = 0;
    *skipped = false;
    for (;;) {
        size_t e = i;
        while (e + sl <= n && memcmp(s + e, sep, sl) != 0) {
            e++;
        }
        bool last_piece = e + sl > n;
        if (last_piece) {
            e = n;
        }
        size_t len = md_strip_generics(s + i, e - i);
        if (pieces == 0 && !*skipped && skip_first && !last_piece &&
            md_in_list(s + i, len, skip_first, false)) {
            *skipped = true;
        } else {
            if (!md_ident(s + i, len)) {
                return false;
            }
            if (w > 0) {
                s[w++] = '.';
            }
            memmove(s + w, s + i, len);
            w += len;
            pieces++;
        }
        if (last_piece) {
            break;
        }
        i = e + sl;
    }
    s[w] = '\0';
    if (pieces < PAIR_LEN) {
        return false;
    }
    md_shape(out, CBM_DOCLINK_MD_QUALIFIED, s, NULL, 0, 0);
    return true;
}

/* `main.rs::run`: a file name with a member (an identifier). */
static bool md_filename_member(char *s, char *sep, uint32_t first, uint32_t last,
                               CBMDocLinkMdPath *out) {
    const char *head = s;
    size_t head_len = (size_t)(sep - s);
    const char *tail = sep + PAIR_LEN;
    const char *tail_last = tail;
    for (const char *t = strstr(tail, "::"); t; t = strstr(t + PAIR_LEN, "::")) {
        tail_last = t + PAIR_LEN;
    }
    const char *hd = head_len ? memchr(head, '.', head_len) : NULL;
    if (!hd || !md_ident(tail_last, strlen(tail_last)) || !md_all_of(head, head_len, "./\\-")) {
        return false;
    }
    const char *ext = head;
    for (size_t i = head_len; i > 0; i--) {
        if (head[i - SKIP_ONE] == '.') {
            ext = head + i;
            break;
        }
    }
    size_t ext_len = head_len - (size_t)(ext - head);
    for (size_t i = 0; i < ext_len; i++) {
        if (ext[i] < 'a' || ext[i] > 'z') {
            return false;
        }
    }
    if (ext_len == 0) {
        return false;
    }
    *sep = '\0';
    md_shape(out, CBM_DOCLINK_MD_FILENAME, s, tail[0] ? tail : NULL, first, last);
    return true;
}

/* One `mark` between a head `[A-Za-z_][\w.]*` and a tail `[A-Za-z_]\w*`
 * (`dotted_tail`: `[A-Za-z_][\w.]*`): the shapes of `Type#member` and of
 * `module:attr`. */
static bool md_head_mark_tail(const char *s, size_t n, char mark, bool dotted_tail) {
    const char *m = memchr(s, mark, n);
    if (!m || m == s || memchr(m + SKIP_ONE, mark, n - (size_t)(m - s) - SKIP_ONE)) {
        return false;
    }
    unsigned char c0 = (unsigned char)s[0];
    size_t head = (size_t)(m - s);
    size_t tail = n - head - SKIP_ONE;
    if (tail == 0) {
        return false;
    }
    unsigned char t0 = (unsigned char)m[SKIP_ONE];
    return (md_alpha(c0) || c0 == '_') && md_all_of(s, head, ".") && (md_alpha(t0) || t0 == '_') &&
           md_all_of(m + SKIP_ONE, tail, dotted_tail ? "." : "");
}

/* No slash: a file name, a special file, `file.ext::member`, or a qualified
 * name. A bare name is neither: at most a suggestion, never a link. */
static bool md_classify_name(char *s, size_t n, CBMDocLinkMdPath *out) {
    if (md_in_list(s, n, MD_SPECIAL_FILES, false)) {
        md_shape(out, CBM_DOCLINK_MD_FILENAME, s, NULL, 0, 0);
        return true;
    }
    uint32_t first = 0;
    uint32_t last = 0;
    size_t cut = n;
    if (md_line_suffix(s, &cut, &first, &last)) {
        const char *dot = NULL;
        for (size_t i = cut; i > 0; i--) {
            if (s[i - SKIP_ONE] == '.') {
                dot = s + i - SKIP_ONE;
                break;
            }
        }
        size_t ext_len = dot ? cut - (size_t)(dot - s) - SKIP_ONE : 0;
        if (dot && md_in_list(dot + SKIP_ONE, ext_len, MD_FILE_EXT, true)) {
            n = cut;
            s[n] = '\0';
        } else {
            first = 0;
            last = 0;
        }
    }
    bool skipped = false;
    if (strstr(s, "::")) {
        /* identifiers only: a qualified name; otherwise perhaps `file.ext::member` */
        char copy[MD_SPAN_MAX + SKIP_ONE];
        memcpy(copy, s, n + SKIP_ONE);
        if (md_join(s, n, "::", MD_RUST_ROOTS, &skipped, out)) {
            return true;
        }
        memcpy(s, copy, n + SKIP_ONE);
        return md_filename_member(s, strstr(s, "::"), first, last, out);
    }
    if (md_has(s, n, '#')) {
        if (!md_head_mark_tail(s, n, '#', false)) {
            return false;
        }
        s[strcspn(s, "#")] = '.';
        return md_join(s, n, ".", NULL, &skipped, out);
    }
    if (md_has(s, n, '\\')) {
        while (n > 0 && s[n - SKIP_ONE] == '\\') {
            n--;
        }
        size_t lead = 0;
        while (lead < n && s[lead] == '\\') {
            lead++;
        }
        return md_join(s + lead, n - lead, "\\", NULL, &skipped, out);
    }
    if (md_has(s, n, ':')) {
        if (!md_head_mark_tail(s, n, ':', true)) {
            return false;
        }
        s[strcspn(s, ":")] = '.';
        if (!md_join(s, n, ".", NULL, &skipped, out)) {
            return false;
        }
        out->colon = true;
        return true;
    }
    if (!md_has(s, n, '.')) {
        return false;
    }
    if (s[0] == '.') {
        const char *rest = s + SKIP_ONE;
        size_t rest_len = n - SKIP_ONE;
        if (md_ident(rest, rest_len) && !md_in_list(rest, rest_len, MD_FILE_EXT, true)) {
            return false; /* `.member` */
        }
        if (md_all_of(rest, rest_len, ".-")) {
            md_shape(out, CBM_DOCLINK_MD_FILENAME, s, NULL, 0, 0);
            return true;
        }
        return false;
    }
    const char *dot = strrchr(s, '.');
    const char *ext = dot + SKIP_ONE;
    size_t ext_len = n - (size_t)(ext - s);
    if (md_in_list(ext, ext_len, MD_FILE_EXT, true) && md_all_of(s, n, ".+-")) {
        size_t stem_len = (size_t)(dot - s);
        bool one_dot = memchr(s, '.', stem_len) == NULL;
        if (one_dot && s[0] >= 'A' && s[0] <= 'Z' && md_eq_ci(ext, ext_len, "js")) {
            return false; /* `Node.js`: a product */
        }
        bool mod_sum = md_eq_ci(ext, ext_len, "mod") || md_eq_ci(ext, ext_len, "sum");
        if (!mod_sum || (stem_len == PAIR_LEN && memcmp(s, "go", PAIR_LEN) == 0)) {
            md_shape(out, CBM_DOCLINK_MD_FILENAME, s, NULL, first, last);
            return true;
        }
    }
    if (!md_join(s, n, ".", MD_INSTANCE_ROOTS, &skipped, out)) {
        return false;
    }
    out->instance = skipped; /* `self.x`: the qualifier is an instance, not a type */
    return true;
}

bool cbm_doclink_md_classify_span(const char *text, size_t len, char *buf, size_t cap,
                                  CBMDocLinkMdPath *out) {
    md_shape(out, CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0);
    if (!text || !buf) {
        return false;
    }
    while (len > 0 && md_blank((unsigned char)text[0])) {
        text++;
        len--;
    }
    while (len > 0 && md_blank((unsigned char)text[len - SKIP_ONE])) {
        len--;
    }
    if (len == 0 || len > MD_SPAN_MAX || len + SKIP_ONE > cap || md_has(text, len, '\n')) {
        return false;
    }
    memcpy(buf, text, len);
    buf[len] = '\0';
    char *s = buf;
    size_t n = len;
    if (md_in_list(s, n, MD_DOMAINS, true) || md_is_url(s, n)) {
        return false;
    }
    bool only_slashes = true;
    for (size_t i = 0; i < n; i++) {
        only_slashes = only_slashes && s[i] == '/';
    }
    if (only_slashes) {
        return false;
    }
    if (n > SKIP_ONE && strchr("$%>#", s[0]) && s[SKIP_ONE] == ' ') {
        return false; /* a shell prompt */
    }
    if (n > SKIP_ONE && s[0] == '-' &&
        (md_alpha((unsigned char)s[SKIP_ONE]) || s[SKIP_ONE] == '-')) {
        return false; /* an option */
    }
    if (s[0] == '$' ||
        (n >= PAIR_LEN && (s[0] == '"' || s[0] == '\'') && s[n - SKIP_ONE] == s[0]) ||
        (s[0] == '<' && s[n - SKIP_ONE] == '>')) {
        return false; /* a variable, a string, markup */
    }
    size_t eq = 0;
    while (eq < n && (md_alnum((unsigned char)s[eq]) || s[eq] == '_')) {
        eq++;
    }
    if (eq > 0 && eq < n && s[eq] == '=' && !md_digit((unsigned char)s[0]) && !md_has(s, n, ' ')) {
        return false; /* NAME=value */
    }
    size_t callee = md_callee(s, n, true);
    if (callee) {
        n = callee;
        s[n] = '\0';
    }
    if (md_has(s, n, ' ') || md_has(s, n, '\t')) {
        return false; /* a declaration, a command line, a snippet */
    }
    if (s[0] == '@') {
        size_t slash = 0;
        while (slash < n && s[slash] != '/') {
            slash++;
        }
        if (slash < n && slash > SKIP_ONE) {
            return false; /* `@scope/package`: an import path (a later rung) */
        }
        s++;
        n--;
        if (n == 0) {
            return false;
        }
    }
    callee = md_callee(s, n, false);
    if (callee) {
        n = callee;
        s[n] = '\0';
    }
    while (n > 0 && strchr(";,:", s[n - SKIP_ONE])) {
        n--;
    }
    if (n > SKIP_ONE && s[n - SKIP_ONE] == '!') {
        n--;
    }
    while (n > 0 && s[n - SKIP_ONE] == '?') {
        n--;
    }
    n = md_strip_generics(s, n);
    while (n > 0 && strchr("&*~", s[0])) {
        s++;
        n--;
    }
    if (n >= PAIR_LEN && s[0] == ':' && s[SKIP_ONE] == ':') {
        s += PAIR_LEN;
        n -= PAIR_LEN;
    }
    if (n == 0) {
        return false;
    }
    s[n] = '\0';
    size_t first_slash = 0;
    while (first_slash < n && s[first_slash] != '/') {
        first_slash++;
    }
    bool slash_path = first_slash < n && !strstr(s, "::");
    if (first_slash < n && !slash_path) {
        /* `a::b/c`: a "::" after the first slash still makes a path */
        char *dc = strstr(s, "::");
        slash_path = dc && (size_t)(dc - s) > first_slash;
    }
    bool windows_path = false;
    if (!slash_path && md_has(s, n, '\\')) {
        const char *bs = strrchr(s, '\\');
        size_t tail = n - (size_t)(bs - s) - SKIP_ONE;
        windows_path = tail > 0 && md_all_of(bs + SKIP_ONE, tail, ".-") && md_ext_ok(bs, tail + 1);
    }
    if (slash_path || windows_path) {
        return md_classify_path(s, n, out);
    }
    return md_classify_name(s, n, out);
}

/* ── Position lists ──────────────────────────────────────────────── */

typedef struct {
    int32_t *v;
    int n;
    int cap;
} md_list_t;

static bool md_push_int(md_list_t *l, int32_t x) {
    if (l->n == l->cap) {
        if (l->cap > INT32_MAX / PAIR_LEN) {
            return false;
        }
        int ncap = l->cap ? l->cap * PAIR_LEN : MD_LIST_INIT;
        int32_t *grown =
            (int32_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, l->v, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        l->v = grown;
        l->cap = ncap;
    }
    l->v[l->n++] = x;
    return true;
}

static bool md_push_pair(md_list_t *l, int32_t a, int32_t b) {
    return md_push_int(l, a) && md_push_int(l, b);
}

static void md_list_free(md_list_t *l) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, l->v);
    l->v = NULL;
    l->n = 0;
    l->cap = 0;
}

/* Index of the first value >= x in an ascending list. */
static int md_lower_bound(const md_list_t *l, int32_t x) {
    int lo = 0;
    int hi = l->n;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (l->v[mid] < x) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* The first value >= x, or -1. */
static int32_t md_next_at(const md_list_t *l, int32_t x) {
    int i = md_lower_bound(l, x);
    return i < l->n ? l->v[i] : CBM_NOT_FOUND;
}

/* ── The scan ────────────────────────────────────────────────────── */

typedef struct {
    uint32_t line;
    const char *qn;
    /* Another heading of the file has the same qualified name: the graph keeps
     * one node for both, which owns only one of the headings, so this
     * section's references belong to the file. */
    bool shared_qn;
} md_section_t;

typedef struct {
    bool in_front;
    bool in_fence;
    bool in_comment;
    char fence_char;
    int fence_len;
} md_state_t;

typedef struct {
    CBMExtractCtx *ctx;
    md_section_t *secs;
    int nsecs;
    char *buf; /* the current line, masked in place */
    int len;
    uint32_t line;
    char *span_buf;   /* classification scratch, MD_SPAN_MAX + 1 bytes */
    md_list_t runs;   /* backtick runs: start, length */
    md_list_t by_len; /* the runs as (length, start), sorted */
    md_list_t ranges; /* ranges to mask: start, end */
    md_list_t stack;  /* open '[' while matching */
    md_list_t cands;  /* link candidates: '[', ']' */
    md_list_t opens;  /* '(' */
    md_list_t closes; /* ')' */
    md_list_t bal;    /* each ')' as (depth before it, position), sorted */
    md_list_t gts;    /* '>' */
    md_list_t blanks; /* runs of spaces and tabs: start, end */
    bool failed;
} md_scan_t;

static int md_section_cmp(const void *a, const void *b) {
    const md_section_t *x = (const md_section_t *)a;
    const md_section_t *y = (const md_section_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    return strcmp(x->qn, y->qn);
}

static int md_section_ptr_qn_cmp(const void *a, const void *b) {
    const md_section_t *x = *(const md_section_t *const *)a;
    const md_section_t *y = *(const md_section_t *const *)b;
    return strcmp(x->qn, y->qn);
}

static int md_pair_cmp(const void *a, const void *b) {
    const int32_t *x = (const int32_t *)a;
    const int32_t *y = (const int32_t *)b;
    if (x[0] != y[0]) {
        return x[0] < y[0] ? -1 : 1;
    }
    return (x[SKIP_ONE] > y[SKIP_ONE]) - (x[SKIP_ONE] < y[SKIP_ONE]);
}

static void md_fail(md_scan_t *s) {
    s->failed = true;
    s->ctx->result->doc_links.failed = true;
}

/* Text that may be a reference: well-formed UTF-8 (no overlong form, no
 * surrogate, nothing above U+10FFFF) without control bytes. */
static bool md_clean_text(const char *t, int n) {
    int i = 0;
    while (i < n) {
        unsigned char c = (unsigned char)t[i];
        if (c < UTF8_CTRL_END || c == UTF8_DEL) {
            return false;
        }
        int need;
        unsigned char lo = UTF8_CONT_MIN;
        unsigned char hi = UTF8_CONT_MAX;
        if (c < UTF8_ASCII_END) {
            need = 0;
        } else if (c >= UTF8_LEAD2_MIN && c <= UTF8_LEAD2_MAX) {
            need = SKIP_ONE;
        } else if (c >= UTF8_LEAD3_MIN && c <= UTF8_LEAD3_MAX) {
            need = PAIR_LEN;
            lo = c == UTF8_LEAD3_MIN ? UTF8_E0_SECOND_MIN : lo;      /* no overlong form */
            hi = c == UTF8_SURROGATE_LEAD ? UTF8_ED_SECOND_MAX : hi; /* no surrogate */
        } else if (c >= UTF8_LEAD4_MIN && c <= UTF8_LEAD4_MAX) {
            need = PAIR_LEN + SKIP_ONE;
            lo = c == UTF8_LEAD4_MIN ? UTF8_F0_SECOND_MIN : lo;
            hi = c == UTF8_LEAD4_MAX ? UTF8_F4_SECOND_MAX : hi;
        } else {
            return false;
        }
        if (i + need >= n + (need == 0)) {
            return false;
        }
        for (int k = SKIP_ONE; k <= need; k++) {
            unsigned char cc = (unsigned char)t[i + k];
            unsigned char kl = k == SKIP_ONE ? lo : UTF8_CONT_MIN;
            unsigned char kh = k == SKIP_ONE ? hi : UTF8_CONT_MAX;
            if (cc < kl || cc > kh) {
                return false;
            }
        }
        i += need + SKIP_ONE;
    }
    return true;
}

static void md_emit(md_scan_t *s, int syntax, const char *text, int n) {
    while (n > 0 && md_blank((unsigned char)text[0])) {
        text++;
        n--;
    }
    while (n > 0 && md_blank((unsigned char)text[n - SKIP_ONE])) {
        n--;
    }
    if (n <= 0 || memchr(text, MD_MASK, (size_t)n) || !md_clean_text(text, n)) {
        return;
    }
    int lo = 0;
    int hi = s->nsecs;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (s->secs[mid].line <= s->line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    const md_section_t *sec = lo > 0 ? &s->secs[lo - SKIP_ONE] : NULL;
    if (sec && sec->shared_qn) {
        sec = NULL; /* the file, not the node another heading owns */
    }
    char *raw = cbm_arena_strndup(s->ctx->arena, text, (size_t)n);
    if (!raw) {
        md_fail(s);
        return;
    }
    CBMDocLink link = {
        .source_qn = sec ? sec->qn : (s->ctx->module_qn ? s->ctx->module_qn : ""),
        .raw = raw,
        .line = s->line,
        .def_line = sec ? sec->line : SKIP_ONE,
        .syntax = (uint16_t)syntax,
        .flags = sec ? 0 : CBM_DOCLINK_FLAG_FILE,
    };
    cbm_doclinks_push(&s->ctx->result->doc_links, s->ctx->arena, link);
}

/* A directory written as one name (`doc/`, `/docs`): no parent says whose it is. */
static bool md_one_dir(const CBMDocLinkMdPath *p) {
    if (p->shape != CBM_DOCLINK_MD_DIR || !p->path) {
        return false;
    }
    const char *s = p->path + (p->path[0] == '/');
    const char *slash = strchr(s, '/');
    return !slash || !slash[SKIP_ONE];
}

/* A code span's text: a reference when it names a path or a qualified name.
 * A name alone -- a file (`config.yaml`) or a directory (`doc/`) -- is its own
 * family: in the held-out audits it was the reader's own file or another
 * project's directory as often as this repository's. */
static void md_emit_span(md_scan_t *s, const char *text, int n) {
    CBMDocLinkMdPath p;
    if (n > 0 &&
        cbm_doclink_md_classify_span(text, (size_t)n, s->span_buf, MD_SPAN_MAX + SKIP_ONE, &p)) {
        bool bare = p.shape == CBM_DOCLINK_MD_FILENAME || md_one_dir(&p);
        md_emit(s,
                p.shape == CBM_DOCLINK_MD_QUALIFIED ? CBM_DOCLINK_MD_CODE_NAME
                : bare                              ? CBM_DOCLINK_MD_BARE_PATH
                                                    : CBM_DOCLINK_MD_CODE_PATH,
                text, n);
    }
}

static void md_mask(md_scan_t *s, int a, int b) {
    if (a < 0) {
        a = 0;
    }
    if (b > s->len) {
        b = s->len;
    }
    if (a < b) {
        memset(s->buf + a, MD_MASK, (size_t)(b - a));
    }
}

static int md_find(const char *buf, int len, int from, const char *needle, bool ci) {
    int k = (int)strlen(needle);
    for (int i = from; i + k <= len; i++) {
        int j = 0;
        while (j < k && (ci ? md_lower((unsigned char)buf[i + j]) : (unsigned char)buf[i + j]) ==
                            (unsigned char)needle[j]) {
            j++;
        }
        if (j == k) {
            return i;
        }
    }
    return CBM_NOT_FOUND;
}

/* HTML <code> spans (tables in READMEs write code that way): their text,
 * without inner tags and with the common entities decoded. */
static void md_html_code(md_scan_t *s) {
    int i = 0;
    while (i < s->len) {
        int open = md_find(s->buf, s->len, i, "<code", true);
        if (open < 0) {
            return;
        }
        int after = open + (int)MD_SLEN("<code");
        int body;
        if (after < s->len && s->buf[after] == '>') {
            body = after + SKIP_ONE;
        } else if (after < s->len && md_blank((unsigned char)s->buf[after])) {
            const char *gt = memchr(s->buf + after, '>', (size_t)(s->len - after));
            if (!gt) {
                return;
            }
            body = (int)(gt - s->buf) + SKIP_ONE;
        } else {
            i = open + SKIP_ONE;
            continue;
        }
        int close = md_find(s->buf, s->len, body, "</code>", true);
        if (close < 0) {
            return;
        }
        /* text without tags, entities decoded; never longer than the body */
        char *text = s->span_buf + MD_SPAN_MAX + SKIP_ONE; /* second half of the scratch */
        int w = 0;
        int k = body;
        while (k < close && w < MD_SPAN_MAX + SKIP_ONE) {
            char c = s->buf[k];
            if (c == '<') {
                int lim = k + MD_TAG_MAX + PAIR_LEN; /* `<`, up to 200 bytes, `>` */
                const char *gt = memchr(s->buf + k + SKIP_ONE, '>',
                                        (size_t)((lim < close ? lim : close) - k - SKIP_ONE));
                if (gt) {
                    k = (int)(gt - s->buf) + SKIP_ONE;
                    continue;
                }
            } else if (c == '&') {
                static const struct {
                    const char *ent;
                    char ch;
                } ents[] = {{"&lt;", '<'},   {"&gt;", '>'},    {"&amp;", '&'},   {"&quot;", '"'},
                            {"&#39;", '\''}, {"&#x27;", '\''}, {"&apos;", '\''}, {"&nbsp;", ' '}};
                bool hit = false;
                for (size_t e = 0; e < sizeof(ents) / sizeof(ents[0]); e++) {
                    int el = (int)strlen(ents[e].ent);
                    if (k + el <= close && memcmp(s->buf + k, ents[e].ent, (size_t)el) == 0) {
                        text[w++] = ents[e].ch;
                        k += el;
                        hit = true;
                        break;
                    }
                }
                if (hit) {
                    continue;
                }
            }
            text[w++] = c;
            k++;
        }
        if (k >= close) {
            md_emit_span(s, text, w); /* a text longer than a span's limit is no name */
        }
        md_mask(s, open, close + (int)MD_SLEN("</code>"));
        i = close + (int)MD_SLEN("</code>");
    }
}

/* Code spans: a run of N backticks up to the next run of exactly N. An
 * escaped first backtick (an odd run of backslashes before it) does not open
 * a span. Closers come from the runs sorted by (length, start). */
static void md_code_spans(md_scan_t *s) {
    s->runs.n = 0;
    s->by_len.n = 0;
    s->ranges.n = 0;
    for (int i = 0; i < s->len;) {
        if (s->buf[i] != '`') {
            i++;
            continue;
        }
        int j = i;
        while (j < s->len && s->buf[j] == '`') {
            j++;
        }
        if (!md_push_pair(&s->runs, i, j - i) || !md_push_pair(&s->by_len, j - i, i)) {
            md_fail(s);
            return;
        }
        i = j;
    }
    int nruns = s->runs.n / PAIR_LEN;
    if (nruns < PAIR_LEN) {
        return;
    }
    qsort(s->by_len.v, (size_t)nruns, sizeof(int32_t) * PAIR_LEN, md_pair_cmp);
    int pos = 0;
    for (int r = 0; r < nruns; r++) {
        int start = s->runs.v[r * PAIR_LEN];
        int len = s->runs.v[(r * PAIR_LEN) + SKIP_ONE];
        if (start < pos) {
            continue;
        }
        int bs = 0;
        while (start - bs > 0 && s->buf[start - bs - SKIP_ONE] == '\\') {
            bs++;
        }
        if (bs % PAIR_LEN) {
            start++;
            len--;
            if (len == 0) {
                continue;
            }
        }
        /* the first run of exactly `len` starting at or after start + len */
        int lo = 0;
        int hi = nruns;
        int32_t key[PAIR_LEN] = {len, start + len};
        while (lo < hi) {
            int mid = lo + ((hi - lo) / PAIR_LEN);
            if (md_pair_cmp(&s->by_len.v[mid * PAIR_LEN], key) < 0) {
                lo = mid + SKIP_ONE;
            } else {
                hi = mid;
            }
        }
        if (lo < nruns && s->by_len.v[lo * PAIR_LEN] == len) {
            int close = s->by_len.v[(lo * PAIR_LEN) + SKIP_ONE];
            int a = start + len;
            int b = close;
            if (b - a >= PAIR_LEN && s->buf[a] == ' ' && s->buf[b - SKIP_ONE] == ' ') {
                bool all_space = true;
                for (int k = a; k < b && all_space; k++) {
                    all_space = s->buf[k] == ' ';
                }
                if (!all_space) {
                    a++;
                    b--;
                }
            }
            md_emit_span(s, s->buf + a, b - a);
            if (!md_push_pair(&s->ranges, start, close + len)) {
                md_fail(s);
                return;
            }
            pos = close + len;
        } else {
            pos = start + len;
        }
    }
    for (int i = 0; i < s->ranges.n; i += PAIR_LEN) {
        md_mask(s, s->ranges.v[i], s->ranges.v[i + SKIP_ONE]);
    }
}

/* `blanks` holds the space/tab runs as start, end, start, end, ... -- one
 * strictly ascending list. The first value above i: an END when i lies in a
 * run (its index is odd), a START (or nothing) when it does not. */
static int md_blank_after(const md_scan_t *s, int i) {
    return md_lower_bound(&s->blanks, i + SKIP_ONE);
}

/* The first space or tab at or after i (the line's end when there is none). */
static int md_next_blank(const md_scan_t *s, int i) {
    int k = md_blank_after(s, i);
    if (k % PAIR_LEN == SKIP_ONE) {
        return i;
    }
    return k < s->blanks.n ? s->blanks.v[k] : s->len;
}

/* The first byte at or after i that is no space or tab. */
static int md_skip_blank(const md_scan_t *s, int i) {
    int k = md_blank_after(s, i);
    return k % PAIR_LEN == SKIP_ONE ? s->blanks.v[k] : i;
}

/* Where a link destination that starts at t ends: at the first space or tab,
 * or at the first ')' at the destination's own parenthesis depth 0. */
static int md_dest_end(const md_scan_t *s, int t) {
    int depth = md_lower_bound(&s->opens, t) - md_lower_bound(&s->closes, t);
    int end = md_next_blank(s, t);
    /* the first ')' at or after t with the depth t had */
    int nb = s->bal.n / PAIR_LEN;
    int lo = 0;
    int hi = nb;
    int32_t key[PAIR_LEN] = {depth, t};
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (md_pair_cmp(&s->bal.v[mid * PAIR_LEN], key) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    if (lo < nb && s->bal.v[lo * PAIR_LEN] == depth) {
        int p = s->bal.v[(lo * PAIR_LEN) + SKIP_ONE];
        if (p < end) {
            end = p;
        }
    }
    return end;
}

static bool md_line_lists(md_scan_t *s) {
    s->stack.n = 0;
    s->cands.n = 0;
    s->opens.n = 0;
    s->closes.n = 0;
    s->bal.n = 0;
    s->gts.n = 0;
    s->blanks.n = 0;
    int depth = 0;
    for (int i = 0; i < s->len; i++) {
        char c = s->buf[i];
        bool ok = true;
        switch (c) {
        case '[':
            ok = md_push_int(&s->stack, i);
            break;
        case ']':
            if (s->stack.n > 0) {
                int o = s->stack.v[--s->stack.n];
                if (i + SKIP_ONE < s->len && s->buf[i + SKIP_ONE] == '(') {
                    ok = md_push_pair(&s->cands, o, i);
                }
            }
            break;
        case '(':
            ok = md_push_int(&s->opens, i);
            depth++;
            break;
        case ')':
            ok = md_push_int(&s->closes, i) && md_push_pair(&s->bal, depth, i);
            depth--;
            break;
        case '>':
            ok = md_push_int(&s->gts, i);
            break;
        case ' ':
        case '\t': {
            int j = i;
            while (j < s->len && md_sp_tab((unsigned char)s->buf[j])) {
                j++;
            }
            ok = md_push_pair(&s->blanks, i, j);
            i = j - SKIP_ONE;
            break;
        }
        default:
            break;
        }
        if (!ok) {
            return false;
        }
    }
    if (s->bal.n > PAIR_LEN) {
        qsort(s->bal.v, (size_t)(s->bal.n / PAIR_LEN), sizeof(int32_t) * PAIR_LEN, md_pair_cmp);
    }
    return true;
}

static void md_link_target(md_scan_t *s, int a, int b, bool image) {
    while (a < b && md_blank((unsigned char)s->buf[a])) {
        a++;
    }
    while (b > a && md_blank((unsigned char)s->buf[b - SKIP_ONE])) {
        b--;
    }
    if (a >= b || image || s->buf[a] == '#' || memchr(s->buf + a, MD_MASK, (size_t)(b - a)) ||
        md_is_url(s->buf + a, (size_t)(b - a))) {
        return; /* empty, an image, an in-page anchor, masked text, a URL */
    }
    md_emit(s, CBM_DOCLINK_MD_LINK, s->buf + a, b - a);
}

/* Inline links and images `[text](dest "title")`. */
static void md_links(md_scan_t *s) {
    if (!md_line_lists(s)) {
        md_fail(s);
        return;
    }
    s->ranges.n = 0;
    int pos = 0;
    int floor = 0;
    for (int c = 0; c < s->cands.n; c += PAIR_LEN) {
        int o = s->cands.v[c];
        int k = s->cands.v[c + SKIP_ONE];
        if (k < pos || o < floor) {
            continue;
        }
        int t = k + PAIR_LEN;
        while (t < s->len && s->buf[t] == ' ') {
            t++;
        }
        int ta;
        int tb;
        int after;
        if (t < s->len && s->buf[t] == '<') {
            int e = md_next_at(&s->gts, t);
            if (e < 0) {
                pos = k + PAIR_LEN;
                continue;
            }
            ta = t + SKIP_ONE;
            tb = e;
            after = e + SKIP_ONE;
        } else {
            int e = md_dest_end(s, t);
            ta = t;
            tb = e;
            after = e;
        }
        int close = md_next_at(&s->closes, after);
        if (close < 0) {
            pos = k + PAIR_LEN;
            continue;
        }
        int q = md_skip_blank(s, after);
        if (q < close && !strchr("\"'(", s->buf[q])) {
            pos = k + PAIR_LEN;
            continue;
        }
        bool image = o > 0 && s->buf[o - SKIP_ONE] == '!';
        md_link_target(s, ta, tb, image);
        if (!md_push_pair(&s->ranges, o - (image ? SKIP_ONE : 0), close + SKIP_ONE)) {
            md_fail(s);
            return;
        }
        pos = close + SKIP_ONE;
        floor = pos;
    }
    for (int i = 0; i < s->ranges.n; i += PAIR_LEN) {
        md_mask(s, s->ranges.v[i], s->ranges.v[i + SKIP_ONE]);
    }
}

/* A link reference definition `[label]: dest` (the whole line). */
static void md_refdef(md_scan_t *s) {
    int i = 0;
    while (i < s->len && i < MD_INDENT_MAX && md_blank((unsigned char)s->buf[i])) {
        i++;
    }
    if (i >= s->len || s->buf[i] != '[') {
        return;
    }
    const char *rb = memchr(s->buf + i + SKIP_ONE, ']', (size_t)(s->len - i - SKIP_ONE));
    if (!rb) {
        return;
    }
    int j = (int)(rb - s->buf);
    if (j == i + SKIP_ONE || j + SKIP_ONE >= s->len || s->buf[j + SKIP_ONE] != ':') {
        return;
    }
    int k = j + PAIR_LEN;
    while (k < s->len && md_blank((unsigned char)s->buf[k])) {
        k++;
    }
    if (k < s->len && s->buf[k] == '<') {
        k++;
    }
    int e = k;
    while (e < s->len && !md_blank((unsigned char)s->buf[e]) && s->buf[e] != '>') {
        e++;
    }
    if (e > k) {
        md_link_target(s, k, e, false);
    }
    md_mask(s, 0, s->len);
}

static bool md_url_stop(unsigned char c) {
    return md_blank(c) || c == '<' || c == '>' || c == '"' || c == '\'' || c == '`' ||
           c == MD_MASK || c == ')' || c == ']' || c == '|';
}

static bool md_path_stop(unsigned char c) {
    return md_blank(c) || strchr("()[]{}<>\"',;:`*|", (int)c) != NULL || c == MD_MASK;
}

/* Bare URLs are masked (they are no repository paths); then every token with
 * a slash and a file extension is a bare path. */
static void md_urls_and_paths(md_scan_t *s) {
    static const char *const schemes[MD_URL_SCHEMES] = {"http://", "https://", "ftp://"};
    for (int i = 0; i < s->len; i++) {
        char c = s->buf[i];
        if (c != 'h' && c != 'f') {
            continue;
        }
        for (int k = 0; k < MD_URL_SCHEMES; k++) {
            int sl = (int)strlen(schemes[k]);
            if (i + sl < s->len && memcmp(s->buf + i, schemes[k], (size_t)sl) == 0 &&
                !md_url_stop((unsigned char)s->buf[i + sl])) {
                int j = i + sl;
                while (j < s->len && !md_url_stop((unsigned char)s->buf[j])) {
                    j++;
                }
                int e = j;
                while (e > i && strchr(".,;:!?", s->buf[e - SKIP_ONE])) {
                    e--;
                }
                md_mask(s, i, e);
                i = j - SKIP_ONE;
                break;
            }
        }
    }
    for (int i = 0; i < s->len;) {
        if (md_path_stop((unsigned char)s->buf[i])) {
            i++;
            continue;
        }
        int j = i;
        while (j < s->len && !md_path_stop((unsigned char)s->buf[j])) {
            j++;
        }
        int e = j;
        while (e > i && s->buf[e - SKIP_ONE] == '.') {
            e--;
        }
        if (e == i) {
            i = j; /* only dots */
            continue;
        }
        const char *tok = s->buf + i;
        size_t n = (size_t)(e - i);
        bool path = md_has(tok, n, '/') && !(n >= PAIR_LEN && tok[0] == '/' && tok[1] == '/');
        for (size_t k = 0; path && k < n; k++) {
            unsigned char c = (unsigned char)tok[k];
            path = md_alnum(c) || strchr("_-+@~./", (int)c) != NULL;
        }
        if (path) {
            size_t end = n;
            while (end > 0 && tok[end - SKIP_ONE] == '/') {
                end--;
            }
            size_t base = end;
            while (base > 0 && tok[base - SKIP_ONE] != '/') {
                base--;
            }
            const char *last = tok + base;
            size_t last_len = end - base;
            size_t dots = 0;
            for (size_t k = 0; k < last_len; k++) {
                dots += last[k] == '.';
            }
            bool tiny_dotfile = last_len > 0 && last[0] == '.' && dots == SKIP_ONE &&
                                last_len < (size_t)MD_TINY_DOTFILE;
            if (md_ext_ok(last, last_len) && !tiny_dotfile) {
                md_emit(s, CBM_DOCLINK_MD_PATH, tok, (int)n);
            }
        }
        i = j;
    }
}

static void md_inline(md_scan_t *s) {
    md_html_code(s);
    if (!s->failed) {
        md_code_spans(s);
    }
    if (!s->failed) {
        md_links(s);
    }
    if (!s->failed) {
        md_refdef(s);
        md_urls_and_paths(s);
    }
}

/* Leading blanks skipped: the line's content. */
static const char *md_lstrip(const char *p, int len, int *rest) {
    int i = 0;
    while (i < len && md_blank((unsigned char)p[i])) {
        i++;
    }
    *rest = len - i;
    return p + i;
}

static bool md_trim_eq(const char *p, int len, const char *word) {
    int n = 0;
    const char *t = md_lstrip(p, len, &n);
    while (n > 0 && md_blank((unsigned char)t[n - SKIP_ONE])) {
        n--;
    }
    return (size_t)n == strlen(word) && memcmp(t, word, (size_t)n) == 0;
}

/* One line of the document. `raw` is not NUL-terminated. */
static void md_line(md_scan_t *s, md_state_t *st, const char *raw, int len, bool mdx) {
    if (s->line == SKIP_ONE && md_trim_eq(raw, len, "---")) {
        st->in_front = true;
        return;
    }
    if (st->in_front) {
        if (md_trim_eq(raw, len, "---") || md_trim_eq(raw, len, "...")) {
            st->in_front = false;
        }
        return;
    }
    int rest = 0;
    const char *t = md_lstrip(raw, len, &rest);
    if (st->in_fence) {
        int k = 0;
        while (k < rest && t[k] == st->fence_char) {
            k++;
        }
        int tail = k;
        while (tail < rest && md_blank((unsigned char)t[tail])) {
            tail++;
        }
        if (k >= st->fence_len && tail == rest) {
            st->in_fence = false;
        }
        return;
    }
    if (rest >= MD_FENCE_MIN && (t[0] == '`' || t[0] == '~')) {
        int k = 0;
        while (k < rest && t[k] == t[0]) {
            k++;
        }
        if (k >= MD_FENCE_MIN && !md_has(t + k, (size_t)(rest - k), '`')) {
            st->in_fence = true;
            st->fence_char = t[0];
            st->fence_len = k;
            return;
        }
    }
    memcpy(s->buf, raw, (size_t)len);
    s->buf[len] = '\0';
    s->len = len;
    int from = 0;
    if (st->in_comment) {
        int end = md_find(s->buf, s->len, 0, "-->", false);
        if (end < 0) {
            return;
        }
        st->in_comment = false;
        memset(s->buf, ' ', (size_t)end + MD_SLEN("-->"));
        from = end + (int)MD_SLEN("-->");
    }
    for (;;) {
        int a = md_find(s->buf, s->len, from, "<!--", false);
        if (a < 0) {
            break;
        }
        int b = md_find(s->buf, s->len, a + (int)MD_SLEN("<!--"), "-->", false);
        if (b < 0) {
            s->len = a;
            s->buf[a] = '\0';
            st->in_comment = true;
            break;
        }
        memset(s->buf + a, ' ', (size_t)(b + (int)MD_SLEN("-->") - a));
        from = b + (int)MD_SLEN("-->");
    }
    if (mdx &&
        (strncmp(s->buf, "import", MD_SLEN("import")) == 0 ||
         strncmp(s->buf, "export", MD_SLEN("export")) == 0) &&
        s->len > (int)MD_SLEN("import") && md_blank((unsigned char)s->buf[MD_SLEN("import")])) {
        return;
    }
    md_inline(s);
}

/* The file's sections: its Section definitions by start line. False only when
 * memory ran out (a file without headings has none, and that is no failure). */
static bool md_sections(CBMExtractCtx *ctx, CBMArena *a, md_section_t **out, int *count) {
    const CBMDefArray *defs = &ctx->result->defs;
    int n = 0;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        n += d->label && d->qualified_name && strcmp(d->label, "Section") == 0;
    }
    *out = NULL;
    *count = 0;
    if (n == 0) {
        return true;
    }
    md_section_t *secs = (md_section_t *)cbm_arena_alloc(a, (size_t)n * sizeof(*secs));
    if (!secs) {
        return false;
    }
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->label && d->qualified_name && strcmp(d->label, "Section") == 0) {
            secs[(*count)++] =
                (md_section_t){.line = d->start_line, .qn = d->qualified_name, .shared_qn = false};
        }
    }
    /* repeated qualified names, found through a copy sorted by name */
    const md_section_t **by_qn =
        (const md_section_t **)cbm_arena_alloc(a, (size_t)n * sizeof(*by_qn));
    if (!by_qn) {
        return false;
    }
    for (int i = 0; i < *count; i++) {
        by_qn[i] = &secs[i];
    }
    qsort(by_qn, (size_t)*count, sizeof(*by_qn), md_section_ptr_qn_cmp);
    for (int i = SKIP_ONE; i < *count; i++) {
        if (strcmp(by_qn[i]->qn, by_qn[i - SKIP_ONE]->qn) == 0) {
            ((md_section_t *)by_qn[i])->shared_qn = true;
            ((md_section_t *)by_qn[i - SKIP_ONE])->shared_qn = true;
        }
    }
    qsort(secs, (size_t)*count, sizeof(*secs), md_section_cmp);
    *out = secs;
    return true;
}

void cbm_doclink_md_scan_file(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source || ctx->source_len <= 0) {
        return;
    }
    CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
    md_scan_t s = {.ctx = ctx};
    bool have_secs = md_sections(ctx, scratch, &s.secs, &s.nsecs);
    s.buf = (char *)cbm_arena_alloc(scratch, (size_t)ctx->source_len + SKIP_ONE);
    s.span_buf = (char *)cbm_arena_alloc(scratch, (size_t)(MD_SPAN_MAX + SKIP_ONE) * PAIR_LEN);
    if (!s.buf || !s.span_buf || !have_secs) {
        ctx->result->doc_links.failed = true;
        return;
    }
    const char *rel = ctx->rel_path ? ctx->rel_path : "";
    size_t rel_len = strlen(rel);
    bool mdx = rel_len >= MD_SLEN(".mdx") &&
               md_eq_ci(rel + rel_len - MD_SLEN(".mdx"), MD_SLEN(".mdx"), ".mdx");
    md_state_t st = {0};
    const char *src = ctx->source;
    int n = ctx->source_len;
    int pos = 0;
    if (n >= MD_BOM_LEN && memcmp(src, "\xEF\xBB\xBF", MD_BOM_LEN) == 0) {
        pos = MD_BOM_LEN; /* a byte-order mark is no part of the first line */
    }
    while (pos < n && !s.failed) {
        const char *nl = memchr(src + pos, '\n', (size_t)(n - pos));
        int end = nl ? (int)(nl - src) : n;
        int len = end - pos;
        if (len > 0 && src[pos + len - SKIP_ONE] == '\r') {
            len--;
        }
        s.line++;
        md_line(&s, &st, src + pos, len, mdx);
        pos = end + SKIP_ONE;
    }
    md_list_free(&s.runs);
    md_list_free(&s.by_len);
    md_list_free(&s.ranges);
    md_list_free(&s.stack);
    md_list_free(&s.cands);
    md_list_free(&s.opens);
    md_list_free(&s.closes);
    md_list_free(&s.bal);
    md_list_free(&s.gts);
    md_list_free(&s.blanks);
    /* an architecture decision record: its node, facts and supersedes */
    cbm_adr_extract(ctx);
}
