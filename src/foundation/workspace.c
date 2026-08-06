/*
 * workspace.c — workspace boundary policy. See workspace.h for the contract and
 * for why this is only the breadth half of the boundary.
 */
#include "foundation/workspace.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <stdlib.h>

#include <stdio.h>
#include <string.h>

enum {
    /* POSIX: two components, so every top-level tree is refused in one rule with
     * no list to maintain — "/etc", "/home", "/Users", "/var", "/opt", "/srv".
     * Depth alone cannot do this on Windows, where the equivalent system trees
     * and a perfectly ordinary workspace are both one component below the drive
     * ("C:/Windows" and "D:/repos"), so Windows uses one component plus the
     * named system directories below. Windows' top-level set is small and stable
     * enough for a list; POSIX's is not. */
    WS_MIN_DEPTH_POSIX = 2,
    WS_MIN_DEPTH_WINDOWS = 1,
};

static bool ws_is_sep(char c) {
    return c == '/' || c == '\\';
}

/* Length of the volume prefix: "/" on POSIX, "C:/" for a drive, "//srv/share"
 * for a UNC share. Returns 0 when the path is not absolute. */
static size_t ws_volume_prefix_len(const char *path) {
    if (!path || !path[0]) {
        return 0;
    }
    /* UNC: two separators, a host, then a share. */
    if (ws_is_sep(path[0]) && ws_is_sep(path[1])) {
        size_t i = 2;
        while (path[i] && !ws_is_sep(path[i])) { /* host */
            i++;
        }
        while (ws_is_sep(path[i])) {
            i++;
        }
        while (path[i] && !ws_is_sep(path[i])) { /* share */
            i++;
        }
        return i;
    }
    if (ws_is_sep(path[0])) {
        return 1;
    }
    /* Drive-letter root, with or without a trailing separator. */
    if (path[1] == ':' &&
        ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z'))) {
        return ws_is_sep(path[2]) ? 3 : 2;
    }
    return 0;
}

static bool ws_is_unc(const char *path) {
    return path && ws_is_sep(path[0]) && ws_is_sep(path[1]);
}

static bool ws_is_windows_style(const char *path) {
    if (!path || !path[0]) {
        return false;
    }
    return path[1] == ':' &&
           ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z'));
}

int cbm_workspace_path_depth(const char *canonical_path) {
    size_t prefix = ws_volume_prefix_len(canonical_path);
    if (prefix == 0) {
        return 0;
    }
    int depth = 0;
    const char *p = canonical_path + prefix;
    /* macOS firmlinks /etc, /tmp and /var under /private, so canonicalizing any
     * of them adds a component: "/etc" resolves to "/private/etc" and would count
     * as two deep, sailing past a minimum of two — exactly the path the reports
     * demonstrate. Treat a leading "private" as transparent so depth reflects the
     * tree the user named. "/private/tmp/proj" still counts two and is allowed. */
    if (strncmp(p, "private", 7) == 0 && (ws_is_sep(p[7]) || p[7] == '\0')) {
        p += 7;
    }
    while (*p) {
        while (ws_is_sep(*p)) {
            p++;
        }
        if (!*p) {
            break;
        }
        depth++;
        while (*p && !ws_is_sep(*p)) {
            p++;
        }
    }
    return depth;
}

/* Case-insensitive on Windows-style paths, exact elsewhere: NTFS is
 * case-insensitive, so ".SSH" must not slip past a case-sensitive compare. */
static bool ws_component_equals(const char *component, size_t len, const char *name,
                                bool fold_case) {
    if (strlen(name) != len) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        char a = component[i];
        char b = name[i];
        if (fold_case) {
            if (a >= 'A' && a <= 'Z') {
                a = (char)(a - 'A' + 'a');
            }
            if (b >= 'A' && b <= 'Z') {
                b = (char)(b - 'A' + 'a');
            }
        }
        if (a != b) {
            return false;
        }
    }
    return true;
}

/* Directory and file names that carry credentials. Matched against every
 * component, so both "…/.ssh" and "…/.ssh/sub" are refused. Additive by design:
 * a pull request appending a name here needs no design authority. */
static const char *const WS_CREDENTIAL_NAMES[] = {
    ".ssh",
    ".aws",
    ".gnupg",
    ".gpg",
    ".kube",
    ".docker",
    ".netrc",
    "_netrc",
    ".git-credentials",
    ".azure",
    ".gcloud",
    "Keychains",
    ".password-store",
    ".authinfo",
};

/* Windows system trees, matched only as the FIRST component below the drive.
 * Nothing legitimate is indexed inside them at any depth.
 *
 * "Users" is deliberately NOT here. Every Windows user's files live under
 * C:\Users\<name>, so matching it the way credential names are matched — against
 * every component — refuses every ordinary Windows project path. It is handled
 * below as the tree root only, mirroring POSIX where "/Users" is refused but
 * "/Users/dev/projects" is not. */
static const char *const WS_WINDOWS_SYSTEM_TREES[] = {
    "Windows",
    "ProgramData",
    "Program Files",
    "Program Files (x86)",
};

/* Roots that are the tree itself rather than something inside it. */
static const char *const WS_WINDOWS_USER_TREE = "Users";

/* Match only the first component below the volume prefix. */
static bool ws_first_component_matches(const char *path, const char *const *names, size_t count,
                                       bool fold_case) {
    const char *p = path + ws_volume_prefix_len(path);
    while (ws_is_sep(*p)) {
        p++;
    }
    const char *start = p;
    while (*p && !ws_is_sep(*p)) {
        p++;
    }
    size_t len = (size_t)(p - start);
    for (size_t i = 0; i < count; i++) {
        if (ws_component_equals(start, len, names[i], fold_case)) {
            return true;
        }
    }
    return false;
}

static bool ws_any_component_matches(const char *path, const char *const *names, size_t count,
                                     bool fold_case) {
    size_t prefix = ws_volume_prefix_len(path);
    const char *p = path + prefix;
    while (*p) {
        while (ws_is_sep(*p)) {
            p++;
        }
        if (!*p) {
            break;
        }
        const char *start = p;
        while (*p && !ws_is_sep(*p)) {
            p++;
        }
        size_t len = (size_t)(p - start);
        for (size_t i = 0; i < count; i++) {
            if (ws_component_equals(start, len, names[i], fold_case)) {
                return true;
            }
        }
    }
    return false;
}

/* True when b is a or lives under a. Compares on a separator boundary so
 * "/a/bc" is not treated as living under "/a/b". */
static bool ws_is_ancestor_or_equal(const char *a, const char *b) {
    if (!a || !b || !a[0] || !b[0]) {
        return false;
    }
    size_t la = strlen(a);
    while (la > 1 && ws_is_sep(a[la - 1])) {
        la--;
    }
    if (strncmp(a, b, la) != 0) {
        return false;
    }
    return b[la] == '\0' || ws_is_sep(b[la]);
}

static bool ws_paths_equal(const char *a, const char *b) {
    return ws_is_ancestor_or_equal(a, b) && ws_is_ancestor_or_equal(b, a);
}

cbm_ws_verdict_t cbm_workspace_classify_root(const char *canonical_path, const char *home_dir,
                                             const char *cache_dir) {
    if (!canonical_path || !canonical_path[0] || ws_volume_prefix_len(canonical_path) == 0) {
        /* A relative or empty path is not a usable root; refuse it the same way
         * as a volume root rather than letting it fall through as allowed. */
        return CBM_WS_DENY_ABSOLUTE;
    }

    bool windows_style = ws_is_windows_style(canonical_path);
    int depth = cbm_workspace_path_depth(canonical_path);

    /* A volume, drive or share root, whatever the platform. */
    if (depth == 0) {
        return CBM_WS_DENY_ABSOLUTE;
    }

    /* Order matters below, and not for cosmetic reasons.
     *
     * The home directory normally CONTAINS the cache directory, so testing the
     * cache first would report every $HOME as "holds the cache" — and, worse,
     * would make it absolutely denied when the design says a person may override
     * it. Home is therefore classified first.
     *
     * Depth comes before the cache test for the same kind of reason: "/Users" is
     * both too broad and an ancestor of the cache, and "too broad, name a
     * project directory below it" is the reason that actually helps the reader. */
    if (home_dir && home_dir[0] && ws_paths_equal(canonical_path, home_dir)) {
        return CBM_WS_DENY_SENSITIVE;
    }

    /* Below a drive or a UNC share the first component is already user space
     * ("D:/repos", "//srv/share/proj"), so one component is enough there. Below a
     * POSIX root it is a system tree, so require two. */
    int min_depth =
        (windows_style || ws_is_unc(canonical_path)) ? WS_MIN_DEPTH_WINDOWS : WS_MIN_DEPTH_POSIX;
    if (depth < min_depth) {
        return CBM_WS_DENY_TOO_SHALLOW;
    }

    /* No rule here for "this root contains the cache directory".
     *
     * An earlier draft refused such roots outright, on the theory that indexing
     * them would absorb every other project's graph database. Two things make
     * that wrong. The indexer only parses recognised source files, and a graph
     * database is binary SQLite it would never extract; and refusing an entire
     * root is the wrong remedy even where the concern holds — the right one is to
     * not walk the cache. Excluding the cache from discovery is tracked
     * separately; classifying the root is not the place for it.
     *
     * cache_dir stays in the signature because the exclusion work will need it
     * and because callers already have it to hand. */
    (void)cache_dir;

    if (ws_any_component_matches(canonical_path, WS_CREDENTIAL_NAMES,
                                 sizeof(WS_CREDENTIAL_NAMES) / sizeof(WS_CREDENTIAL_NAMES[0]),
                                 windows_style)) {
        return CBM_WS_DENY_SENSITIVE;
    }

    if (windows_style && ws_first_component_matches(canonical_path, WS_WINDOWS_SYSTEM_TREES,
                                                    sizeof(WS_WINDOWS_SYSTEM_TREES) /
                                                        sizeof(WS_WINDOWS_SYSTEM_TREES[0]),
                                                    true)) {
        return CBM_WS_DENY_SENSITIVE;
    }

    /* "C:/Users" is the user tree itself — refuse it as a root, but leave the
     * projects below it alone; that is where Windows users actually work. */
    if (windows_style && depth == 1 &&
        ws_first_component_matches(canonical_path, &WS_WINDOWS_USER_TREE, 1, true)) {
        return CBM_WS_DENY_SENSITIVE;
    }

    return CBM_WS_ALLOW;
}

const char *cbm_workspace_verdict_reason(cbm_ws_verdict_t verdict) {
    switch (verdict) {
    case CBM_WS_ALLOW:
        return "allowed";
    case CBM_WS_DENY_TOO_SHALLOW:
        return "path is too broad to index as one root; name a project directory below it";
    case CBM_WS_DENY_ABSOLUTE:
        return "path is a volume root or holds the codebase-memory cache; it cannot be indexed";
    case CBM_WS_DENY_SENSITIVE:
        return "path is a home or credential directory";
    default:
        break;
    }
    return "refused";
}

bool cbm_workspace_verdict_is_overridable(cbm_ws_verdict_t verdict) {
    return verdict == CBM_WS_DENY_SENSITIVE;
}

/* ── Grant store ──────────────────────────────────────────────────────────── */

enum { WS_LINE_MAX = 4096 };

/* A line may carry a leading '!' meaning "a person explicitly approved this
 * sensitive root". The marker is stored rather than inferred so re-classifying
 * later (a new credential name is added to the list, say) cannot silently
 * upgrade an ordinary grant into a sensitive one. */
static const char WS_SENSITIVE_MARK = '!';

bool cbm_workspace_grant_path(const char *cache_dir, char *out, size_t out_sz) {
    if (!cache_dir || !cache_dir[0] || !out || out_sz == 0) {
        return false;
    }
    int n = snprintf(out, out_sz, "%s/allowed_roots", cache_dir);
    return n > 0 && (size_t)n < out_sz;
}

/* Walk the grant file, invoking visit for each entry. visit returns true to stop.
 * Returns the number of entries seen. */
static int ws_grant_walk(const char *cache_dir,
                         bool (*visit)(const char *root, bool sensitive, void *ctx), void *ctx) {
    char store[WS_LINE_MAX];
    if (!cbm_workspace_grant_path(cache_dir, store, sizeof(store))) {
        return 0;
    }
    FILE *f = cbm_fopen(store, "r");
    if (!f) {
        return 0;
    }
    char line[WS_LINE_MAX];
    int seen = 0;
    while (fgets(line, (int)sizeof(line), f)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0 || line[0] == '#') {
            continue;
        }
        bool sensitive = line[0] == WS_SENSITIVE_MARK;
        const char *root = sensitive ? line + 1 : line;
        if (!root[0]) {
            continue;
        }
        seen++;
        if (visit && visit(root, sensitive, ctx)) {
            break;
        }
    }
    (void)fclose(f);
    return seen;
}

typedef struct {
    const char *candidate;
    bool contained;       /* candidate is at or below some granted root */
    bool exact_sensitive; /* candidate is itself a grant marked sensitive */
} ws_match_t;

static bool ws_match_visit(const char *root, bool sensitive, void *ctx) {
    ws_match_t *m = ctx;
    if (cbm_path_within_root(root, m->candidate)) {
        m->contained = true;
        if (sensitive && ws_paths_equal(root, m->candidate)) {
            m->exact_sensitive = true;
        }
    }
    return false; /* visit every entry: a later one may be the exact match */
}

typedef struct {
    char *out;
    size_t out_sz;
    size_t used;
} ws_list_t;

static bool ws_list_visit(const char *root, bool sensitive, void *ctx) {
    ws_list_t *l = ctx;
    int n = snprintf(l->out + l->used, l->out_sz - l->used, "%s%s\n",
                     sensitive ? "(approved) " : "", root);
    if (n > 0 && (size_t)n < l->out_sz - l->used) {
        l->used += (size_t)n;
    }
    return false;
}

bool cbm_workspace_grant_list(const char *cache_dir, char *out, size_t out_sz) {
    if (!out || out_sz == 0) {
        return false;
    }
    out[0] = '\0';
    ws_list_t l = {out, out_sz, 0};
    return ws_grant_walk(cache_dir, ws_list_visit, &l) > 0;
}

bool cbm_workspace_grant_add(const char *cache_dir, const char *home_dir,
                             const char *canonical_path, bool approve_sensitive, char *err,
                             size_t err_sz) {
    if (err && err_sz) {
        err[0] = '\0';
    }
    if (!cache_dir || !cache_dir[0] || !canonical_path || !canonical_path[0]) {
        if (err) {
            snprintf(err, err_sz, "no path given");
        }
        return false;
    }

    cbm_ws_verdict_t verdict = cbm_workspace_classify_root(canonical_path, home_dir, cache_dir);
    if (verdict != CBM_WS_ALLOW) {
        if (!cbm_workspace_verdict_is_overridable(verdict)) {
            if (err) {
                snprintf(err, err_sz, "%s", cbm_workspace_verdict_reason(verdict));
            }
            return false;
        }
        if (!approve_sensitive) {
            /* Refuse by default and name the flag, rather than asking a question
             * whose answer we cannot authenticate. */
            if (err) {
                snprintf(err, err_sz, "%s; re-run with --approve-sensitive if that is intended",
                         cbm_workspace_verdict_reason(verdict));
            }
            return false;
        }
    }

    /* Already present is success, not a duplicate error. */
    ws_match_t existing = {canonical_path, false, false};
    (void)ws_grant_walk(cache_dir, ws_match_visit, &existing);
    if (existing.contained) {
        return true;
    }

    char store[WS_LINE_MAX];
    if (!cbm_workspace_grant_path(cache_dir, store, sizeof(store))) {
        if (err) {
            snprintf(err, err_sz, "cache path too long");
        }
        return false;
    }
    FILE *f = cbm_fopen(store, "a");
    if (!f) {
        if (err) {
            snprintf(err, err_sz, "cannot write %s", store);
        }
        return false;
    }
    bool sensitive = verdict == CBM_WS_DENY_SENSITIVE;
    (void)fprintf(f, "%s%s\n", sensitive ? "!" : "", canonical_path);
    bool ok = fclose(f) == 0;
    if (!ok && err) {
        snprintf(err, err_sz, "cannot write %s", store);
    }
    return ok;
}

bool cbm_workspace_root_allowed(const char *canonical_path, const char *home_dir,
                                const char *cache_dir, const char *configured_root, char *err,
                                size_t err_sz) {
    if (err && err_sz) {
        err[0] = '\0';
    }
    if (!canonical_path || !canonical_path[0]) {
        if (err) {
            snprintf(err, err_sz, "no repository path given");
        }
        return false;
    }

    ws_match_t match = {canonical_path, false, false};
    int grants = ws_grant_walk(cache_dir, ws_match_visit, &match);

    /* A configured root behaves as an additional grant so existing
     * CBM_ALLOWED_ROOT deployments keep working unchanged. */
    bool configured = configured_root && configured_root[0];
    bool configured_contains = configured && cbm_path_within_root(configured_root, canonical_path);

    /* Containment first when a boundary has actually been declared. A path
     * outside a configured root is best explained as exactly that, and the
     * wording is what callers and the shell contracts already match on. Breadth
     * then applies to paths that ARE inside the declared root but are still too
     * broad to index as one unit. */
    bool boundary_declared = grants > 0 || configured;
    if (boundary_declared && !match.contained && !configured_contains) {
        if (err) {
            /* Keep the "outside the allowed root" wording: changing it broke an
             * assertion whose early return then leaked CBM_ALLOWED_ROOT into
             * every later test in that suite. Guidance is appended, not
             * substituted. */
            snprintf(err, err_sz,
                     "%s is outside the allowed root. To allow it, run: "
                     "codebase-memory-mcp allow-root %s",
                     canonical_path, canonical_path);
        }
        return false;
    }

    cbm_ws_verdict_t verdict = cbm_workspace_classify_root(canonical_path, home_dir, cache_dir);
    if (verdict == CBM_WS_ALLOW) {
        return true;
    }
    /* An explicit human approval recorded for exactly this path is the only thing
     * that lifts a sensitive refusal. Absolute and shallow refusals cannot be
     * lifted at all. */
    if (verdict == CBM_WS_DENY_SENSITIVE && match.exact_sensitive) {
        return true;
    }
    if (err) {
        if (cbm_workspace_verdict_is_overridable(verdict)) {
            snprintf(err, err_sz,
                     "%s: %s. To index it anyway, run: codebase-memory-mcp allow-root "
                     "--approve-sensitive %s",
                     canonical_path, cbm_workspace_verdict_reason(verdict), canonical_path);
        } else {
            snprintf(err, err_sz, "%s: %s", canonical_path, cbm_workspace_verdict_reason(verdict));
        }
    }
    return false;
}

/* ── Environment helpers ──────────────────────────────────────────────────── */

/* Callers should not each re-derive these; a caller that resolved the home
 * directory differently would classify the same path differently. */
const char *cbm_workspace_home_dir(void) {
    const char *home = getenv("HOME");
    if (home && home[0]) {
        return home;
    }
    home = getenv("USERPROFILE");
    return (home && home[0]) ? home : NULL;
}

const char *cbm_workspace_cache_dir(void) {
    return cbm_resolve_cache_dir();
}
