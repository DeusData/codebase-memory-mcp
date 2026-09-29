/*
 * pass_hashuid.c — Java HashUID: the TrackerV2 identity of a declaration,
 * stamped onto the node as a property.
 *
 * What it is
 * ----------
 * TrackerV2 identifies a Java declaration by hashing eight fields joined by
 * '|':
 *
 *   name | signature | path | condition | logical_module | type_id |
 *   language_type_id | duplicate_fingerprint
 *
 * then taking the 32-char lowercase hex MD5 of that UTF-8 string. Aligning
 * cbm's nodes with TrackerV2 means reproducing those bytes exactly: same
 * field order, same spelling of each field, and — easy to get wrong — the
 * empty fields kept as empty, because omitting a separator changes the digest.
 *
 *   "getChangedFileContents|getChangedFileContents(String,String)|src/…/GitClient.java"
 *   "||GitClient|2|2|"
 *          ^ two '|' in a row: empty condition, then the empty duplicate field.
 *
 * Scope
 * -----
 * Java only, and only the four kinds TrackerV2 registers:
 * Class (20), Enum (22), Interface (23), Method (2) / Constructor (3).
 * Everything else on the node — File, Folder, Field, Variable, Decorator,
 * Route, Branch, Project — has no TrackerV2 identity and is left untouched.
 *
 * Three rules are easy to miss and all three are TrackerV2's, not ours:
 *   1. A method with no body (abstract, native, interface declaration) DOES get
 *      a HashUID here, which is a deliberate superset of TrackerV2: that tool
 *      stops at body-bearing declarations, but cbm's graph is used to track a
 *      declaration across versions, and an interface method is a real,
 *      referencable entity that deserves an identity of its own. These nodes
 *      are identifiable by `hasBody:false`, so a comparison against TrackerV2
 *      can put them aside in one filter instead of guessing.
 *   2. A `record` and an `@interface` are not identity-bearing types, and they
 *      do not scope their members either. Their nodes get no HashUID and are
 *      skipped when a member walks its container chain.
 *   3. `logical_module` is the chain of enclosing container SIMPLE names, not a
 *      qualified path: a method in a top-level class reports just the class
 *      name, a class nested in `Outer` reports `Outer`, and a top-level class
 *      reports nothing at all.
 *
 * Why a pass and not the extractor
 * --------------------------------
 * `logical_module` needs the container chain to be complete, which is only
 * true once every file has been extracted. This runs as a predump pass, where
 * the whole graph is present and node properties are still writable.
 */

#include "foundation/constants.h"
#include "foundation/md5.h"

#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Rotating per-thread buffers for log key=value emission, mirroring the
 * itoa_log helper the other passes keep local to avoid growing a foundation-
 * wide formatting API for log output alone. */
static const char *hu_u64(uint64_t v) {
    static _Thread_local char bufs[4][CBM_SZ_32];
    static _Thread_local int slot = 0;
    char *out = bufs[slot];
    slot = (slot + SKIP_ONE) & 3;
    snprintf(out, sizeof(bufs[0]), "%llu", (unsigned long long)v);
    return out;
}

/* TrackerV2's Java registry (identifier_projection.py). */
enum {
    HU_JAVA_LANGUAGE_TYPE_ID = 2,
    HU_TYPE_ID_METHOD = 2,
    HU_TYPE_ID_CONSTRUCTOR = 3,
    HU_TYPE_ID_CLASS = 20,
    HU_TYPE_ID_ENUM = 22,
    HU_TYPE_ID_INTERFACE = 23,
};

/* Properties this pass reads. Written by the extractor-side build_def_props
 * (both pipeline twins) when the language is Java. */
#define HU_PROP_HAS_BODY "\"hasBody\":"
#define HU_PROP_TYPE_KIND "\"javaTypeKind\":\""

/* Bound on the identity string. A Java signature is source text, so it can be
 * long — a nested generic parameter list is not a pathological case. The MD5
 * is streamed in three updates, so this only bounds one field. */
enum { HU_INPUT_BUF = 4096, HU_MODULE_DEPTH_MAX = 64 };

/* ── properties JSON helpers ──────────────────────────────────────────
 *
 * Node properties arrive as a flat JSON object produced by the extractor.
 * Appending is done by hand, exactly as pass_importance.c does, because the
 * pass must stay free of a general JSON dependency. Every write REPLACES an
 * existing value for the same key, so re-running the pass (a second index, a
 * closure-delta rescan) is idempotent rather than accumulating duplicates.
 */

/* Find `"key":` at the top level of `json` and return a pointer to the value,
 * or NULL. The properties object is flat and its string values are escaped by
 * the extractor, so a plain search cannot be fooled by a nested object. */
static const char *hu_find_value(const char *json, const char *key) {
    if (!json || !key) {
        return NULL;
    }
    size_t key_len = strlen(key);
    for (const char *p = json; (p = strstr(p, key)) != NULL; p += key_len) {
        if (p > json && p[-SKIP_ONE] != '{' && p[-SKIP_ONE] != ',') {
            continue; /* a key prefix of a longer key */
        }
        return p + key_len;
    }
    return NULL;
}

/* Value of a string property, copied into `out`. Returns false when absent. */
static bool hu_get_string(const char *json, const char *key, char *out, size_t cap) {
    const char *v = hu_find_value(json, key);
    if (!v || *v != '"') {
        return false;
    }
    const char *s = v + SKIP_ONE;
    const char *end = strchr(s, '"');
    if (!end) {
        return false;
    }
    size_t len = (size_t)(end - s);
    if (len == 0 || len >= cap) {
        return false;
    }
    memcpy(out, s, len);
    out[len] = '\0';
    return true;
}

/* True when a boolean property reads exactly true. */
static bool hu_get_bool(const char *json, const char *key, bool *out) {
    const char *v = hu_find_value(json, key);
    if (!v) {
        return false;
    }
    if (strncmp(v, "true", 4) == 0) {
        *out = true;
        return true;
    }
    if (strncmp(v, "false", 5) == 0) {
        *out = false;
        return true;
    }
    return false;
}

/* The end of the JSON value starting at `v` (used to replace it in place). */
static const char *hu_value_end(const char *v) {
    if (!v) {
        return NULL;
    }
    if (*v == '"') {
        for (const char *p = v + SKIP_ONE; *p; p++) {
            if (*p == '\\' && p[SKIP_ONE]) {
                p++;
            } else if (*p == '"') {
                return p + SKIP_ONE;
            }
        }
        return NULL;
    }
    if (*v == 't' || *v == 'f' || *v == 'n') {
        while (*v && *v != ',' && *v != '}') {
            v++;
        }
        return v;
    }
    if (*v == '[' || *v == '{') {
        char open = *v;
        char close = (open == '[') ? ']' : '}';
        int depth = 0;
        for (const char *p = v; *p; p++) {
            if (*p == '\\' && p[SKIP_ONE]) {
                p++;
                continue;
            }
            if (*p == '"' ) {
                for (p++; *p; p++) {
                    if (*p == '\\' && p[SKIP_ONE]) {
                        p++;
                    } else if (*p == '"') {
                        break;
                    }
                }
                continue;
            }
            if (*p == open) {
                depth++;
            } else if (*p == close) {
                if (--depth == 0) {
                    return p + SKIP_ONE;
                }
            }
        }
        return NULL;
    }
    while (*v && *v != ',' && *v != '}') {
        v++;
    }
    return v;
}

/* Return a heap string: `old` with `"key":quoted_value` set, appending the
 * field when it is absent. NULL when the input is unusable or memory ran out. */
static char *hu_set_json_string(const char *old, const char *key, const char *value) {
    if (!old || !key || !value) {
        return NULL;
    }
    size_t olen = strlen(old);
    if (olen < 2 || old[olen - SKIP_ONE] != '}') {
        return NULL;
    }
    char keybuf[CBM_SZ_64];
    int kn = snprintf(keybuf, sizeof(keybuf), "\"%s\":", key);
    if (kn <= 0 || (size_t)kn >= sizeof(keybuf)) {
        return NULL;
    }

    const char *v = hu_find_value(old, keybuf);
    if (v) {
        const char *vend = hu_value_end(v);
        if (!vend) {
            return NULL;
        }
        /* Sizes are computed, never guessed: `value` is the full identity
         * string, and a long repository path plus a long generic signature
         * comfortably exceeds any fixed fragment buffer. */
        size_t head = (size_t)(v - old);
        size_t vlen = strlen(value);
        size_t tail = strlen(vend);
        char *quoted = (char *)malloc(vlen + 3U);
        if (!quoted) {
            return NULL;
        }
        quoted[0] = '"';
        memcpy(quoted + 1, value, vlen);
        quoted[vlen + 1] = '"';
        quoted[vlen + 2] = '\0';
        size_t qn = vlen + 2U;
        char *neu = (char *)malloc(head + qn + tail + SKIP_ONE);
        if (!neu) {
            free(quoted);
            return NULL;
        }
        memcpy(neu, old, head);
        memcpy(neu + head, quoted, qn);
        memcpy(neu + head + qn, vend, tail + SKIP_ONE);
        free(quoted);
        return neu;
    }

    size_t frag_cap = strlen(key) + strlen(value) + 8U;
    char *frag = (char *)malloc(frag_cap);
    if (!frag) {
        return NULL;
    }
    int fn = snprintf(frag, frag_cap, "%s\"%s\":\"%s\"}", (olen == 2) ? "" : ",", key, value);
    if (fn <= 0 || (size_t)fn >= frag_cap) {
        free(frag);
        return NULL;
    }
    char *neu = (char *)malloc(olen - SKIP_ONE + (size_t)fn + SKIP_ONE);
    if (!neu) {
        free(frag);
        return NULL;
    }
    memcpy(neu, old, olen - SKIP_ONE);
    memcpy(neu + olen - SKIP_ONE, frag, (size_t)fn + SKIP_ONE);
    free(frag);
    return neu;
}

/* Return a heap string: `old` with `"key":<int>` set. */
static char *hu_set_json_int(const char *old, const char *key, int value) {
    if (!old || !key) {
        return NULL;
    }
    char val[CBM_SZ_32];
    snprintf(val, sizeof(val), "%d", value);
    size_t olen = strlen(old);
    if (olen < 2 || old[olen - SKIP_ONE] != '}') {
        return NULL;
    }
    char keybuf[CBM_SZ_64];
    int kn = snprintf(keybuf, sizeof(keybuf), "\"%s\":", key);
    if (kn <= 0 || (size_t)kn >= sizeof(keybuf)) {
        return NULL;
    }
    const char *v = hu_find_value(old, keybuf);
    if (v) {
        const char *vend = hu_value_end(v);
        if (!vend) {
            return NULL;
        }
        size_t head = (size_t)(v - old);
        size_t vlen = strlen(val);
        size_t tail = strlen(vend);
        char *neu = (char *)malloc(head + vlen + tail + SKIP_ONE);
        if (!neu) {
            return NULL;
        }
        memcpy(neu, old, head);
        memcpy(neu + head, val, vlen);
        memcpy(neu + head + vlen, vend, tail + SKIP_ONE);
        return neu;
    }
    char frag[CBM_SZ_64];
    int fn = snprintf(frag, sizeof(frag), "%s\"%s\":%s}", (olen == 2) ? "" : ",", key, val);
    if (fn <= 0 || (size_t)fn >= sizeof(frag)) {
        return NULL;
    }
    char *neu = (char *)malloc(olen - SKIP_ONE + (size_t)fn + SKIP_ONE);
    if (!neu) {
        return NULL;
    }
    memcpy(neu, old, olen - SKIP_ONE);
    memcpy(neu + olen - SKIP_ONE, frag, (size_t)fn + SKIP_ONE);
    return neu;
}

/* Apply a list of (key, value) updates to a node's properties, allocating each
 * intermediate string and freeing the previous one. Best effort: a failed step
 * leaves the node exactly as it was. */
static bool hu_node_set_string(cbm_gbuf_node_t *node, const char *key, const char *value) {
    char *neu = hu_set_json_string(node->properties_json, key, value);
    if (!neu) {
        return false;
    }
    if (cbm_gbuf_node_set_properties_json(node, neu) != 0) {
        free(neu);
        return false;
    }
    free(neu);
    return true;
}

static bool hu_node_set_int(cbm_gbuf_node_t *node, const char *key, int value) {
    char *neu = hu_set_json_int(node->properties_json, key, value);
    if (!neu) {
        return false;
    }
    if (cbm_gbuf_node_set_properties_json(node, neu) != 0) {
        free(neu);
        return false;
    }
    free(neu);
    return true;
}

/* ── one node ────────────────────────────────────────────────────── */

typedef struct {
    CBMHashTable *containers;
    /* "<class>.<method>" → the method's node, for the scopes that have no TYPE
     * node of their own. TrackerV2's container set includes method and
     * constructor declarations: a member of an anonymous class created inside
     * `JGitClient.iterateFileContents` is scoped as
     * `JGitClient.iterateFileContents.$AC_Iterator`, so the walk has to
     * recognise that middle segment as a scope even though nothing in the qn
     * marks it as one. */
    CBMHashTable *member_scopes;
    /* Set for the stamping phase only: the collected candidates and their
     * groups. NULL while collecting. */
    struct hu_build_s *build;
    uint64_t stamped;
    /* Of the stamped nodes, how many declare no body. Reported so the size of
     * the deliberate superset over TrackerV2 is visible in every index log. */
    uint64_t stamped_no_body;
    uint64_t skipped_not_tracker_type;
    uint64_t failed;
} hu_ctx_t;

/* ── container chain (logical_module) ────────────────────────────────
 *
 * Walks the qn upward one segment at a time and keeps the SIMPLE name of every
 * segment that is a container node. Note what this deliberately does NOT do:
 * follow each node's parent_class. cbm's Variable/Field qns are package
 * scoped (`<package>.<name>`), so a trimmed path collides with one of those
 * nodes and drags an unrelated class name into the chain. Trimming the qn and
 * accepting only Class/Interface/Enum keeps the walk honest.
 */

/* Read the `javaTypeKind` value ("record" | "annotation" | "anonymous"), which
 * the extractor writes for the Java types TrackerV2 does not register. Returns
 * false when the node carries none, i.e. it is an ordinary type. */
static bool hu_get_type_kind(const char *props, char *out, size_t cap) {
    if (!props) {
        return false;
    }
    const char *v = hu_find_value(props, HU_PROP_TYPE_KIND);
    if (!v) {
        return false;
    }
    const char *end = strchr(v, '"');
    if (!end) {
        return false;
    }
    size_t len = (size_t)(end - v);
    if (len == 0 || len >= cap) {
        return false;
    }
    memcpy(out, v, len);
    out[len] = '\0';
    return true;
}

/* An identity-bearing type for TrackerV2: a Class/Interface/Enum that is none
 * of `record`, `@interface` or an anonymous class. */
static bool hu_is_tracker_type(const char *props) {
    char kind[CBM_SZ_32];
    return !hu_get_type_kind(props, kind, sizeof(kind));
}

/* Whether the type scopes its members. Everything identity-bearing does, and
 * so does an anonymous class — it is the one kind that scopes without being
 * registered, which is why records/annotations and anonymous classes cannot
 * share a single "is a type" test. */
static bool hu_scopes_members(const char *props) {
    char kind[CBM_SZ_32];
    if (!hu_get_type_kind(props, kind, sizeof(kind))) {
        return true;
    }
    return strcmp(kind, "anonymous") == 0;
}

static const char *hu_container_name(CBMHashTable *containers, const char *qn) {
    if (!containers || !qn || !qn[0]) {
        return NULL;
    }
    return (const char *)cbm_ht_get(containers, qn);
}

/* The name a scope segment contributes: its own simple name when it is a
 * container node, otherwise the last dotted segment for a `member_scopes` hit.
 * Both the start segment and every trimmed prefix go through here. */
static const char *hu_scope_segment_name(const hu_ctx_t *hc, const char *prefix) {
    const char *hit = hu_container_name(hc->containers, prefix);
    if (hit) {
        return hit;
    }
    if (hc->member_scopes && cbm_ht_get(hc->member_scopes, prefix)) {
        const char *dot = strrchr(prefix, '.');
        return dot ? dot + SKIP_ONE : prefix;
    }
    return NULL;
}

/* Build `outer.inner.…` for the containers enclosing `start_qn`. `start_qn` is
 * the container of the declaration itself (its parent_class, or the qn minus
 * its last segment for a type).
 *
 * A method of a top-level class reports that class's name; a top-level class
 * reports "" because the walk starts one level above it and finds no
 * container. That asymmetry is TrackerV2's, and reproducing it is the point. */
static void hu_logical_module(const hu_ctx_t *hc, const char *start_qn, char *out, size_t cap) {
    const char *segments[HU_MODULE_DEPTH_MAX];
    int count = 0;
    char buf[HU_INPUT_BUF];
    int n = snprintf(buf, sizeof(buf), "%s", start_qn ? start_qn : "");
    if (n <= 0 || (size_t)n >= sizeof(buf)) {
        out[0] = '\0';
        return;
    }
    char *cur = buf;
    while (cur && cur[0]) {
        const char *hit = hu_scope_segment_name(hc, cur);
        if (hit && count < HU_MODULE_DEPTH_MAX) {
            segments[count++] = hit;
        }
        char *dot = strrchr(cur, '.');
        if (!dot) {
            break;
        }
        *dot = '\0';
    }

    size_t pos = 0;
    for (int i = count - SKIP_ONE; i >= 0; i--) {
        const char *seg = segments[i];
        size_t len = strlen(seg);
        if (pos > 0) {
            if (pos + SKIP_ONE >= cap) {
                break;
            }
            out[pos++] = '.';
        }
        if (pos + len + SKIP_ONE > cap) {
            break;
        }
        memcpy(out + pos, seg, len);
        pos += len;
    }
    out[pos] = '\0';
}

/* Where the container of `node` starts. For any member that is the node's own
 * parent_class (set for methods and fields); for the type itself it is the qn
 * with its last segment removed. */
static void hu_parent_qn(const cbm_gbuf_node_t *node, const char *parent_class, char *out,
                         size_t cap) {
    if (parent_class && parent_class[0]) {
        snprintf(out, cap, "%s", parent_class);
        return;
    }
    snprintf(out, cap, "%s", node->qualified_name ? node->qualified_name : "");
    /* A member qn carries its parameter list, which may itself contain dots
     * (`foo(java.util.List)`), so the list is cut off before the last segment
     * is dropped — otherwise the "parent" would be a fragment of a type name. */
    char *open = strchr(out, '(');
    if (open) {
        *open = '\0';
    }
    char *dot = strrchr(out, '.');
    if (dot) {
        *dot = '\0';
    } else {
        out[0] = '\0';
    }
}

/* The canonical TrackerV2 signature: the member name followed by the parameter
 * TYPE list — no argument names, no spaces. cbm already carries it as the
 * tail of the member qn (`…GitClient.getChangedFileContents(String,String)`),
 * which the extractor builds from the same raw type text, so the signature
 * cannot drift from the identity it belongs to. */
static const char *hu_canonical_signature(const cbm_gbuf_node_t *node, char *out, size_t cap) {
    const char *qn = node->qualified_name;
    const char *open = qn ? strchr(qn, '(') : NULL;
    int n = snprintf(out, cap, "%s%s", node->name ? node->name : "", open ? open : "()");
    if (n <= 0 || (size_t)n >= cap) {
        return NULL;
    }
    return out;
}

/* ── candidate collection (phase A) ──────────────────────────────────
 *
 * TrackerV2 assigns duplicate_fingerprint per DUPLICATE GROUP: declarations
 * sharing the first seven fields get `<sha256(full text)><index>` — index taken
 * in (start_line, end_line) order — and a group of one gets "". The
 * fingerprint is part of the hash input, so members of a group that would
 * otherwise be identical end up with distinct hashuids. That makes the
 * fingerprint a GROUP property, not a node property: nothing may be stamped
 * until every member has been seen. Hence two phases.
 */
typedef struct {
    cbm_gbuf_node_t *node;
    char *prefix;          /* "<f1>|...|<f7>|" — owned, and the group key */
    char *module;          /* owned */
    const char *decl_hash; /* owned; NULL when the extractor recorded none */
    /* OWNED. A Java method's canonical signature is assembled in a caller
     * stack buffer, so it cannot be borrowed: phase B runs after that frame is
     * gone (ASan flagged exactly this as stack-use-after-scope). */
    char *signature;
    int type_id;
    /* Declaration had no body (interface / abstract / native). Stamped like any
     * other member — see rule 1 at the top of this file — and counted so the
     * pass log reports how far the identity set extends past TrackerV2. */
    bool no_body;
    uint32_t start_line;
    uint32_t end_line;
    int64_t id;
} hu_entry_t;

typedef struct {
    int *items; /* indices into hu_build_t.entries */
    int count;
    int cap;
} hu_group_t;

typedef struct hu_build_s {
    hu_entry_t *entries;
    int count;
    int cap;
    CBMHashTable *groups; /* prefix → hu_group_t* (keys owned) */
    uint64_t missing_hash;
} hu_build_t;

static hu_group_t *hu_group_for(hu_build_t *hb, const char *prefix) {
    hu_group_t *group = (hu_group_t *)cbm_ht_get(hb->groups, prefix);
    if (group) {
        return group;
    }
    group = (hu_group_t *)calloc(CBM_ALLOC_ONE, sizeof(*group));
    if (!group) {
        return NULL;
    }
    char *key = (char *)malloc(strlen(prefix) + SKIP_ONE);
    if (!key) {
        free(group);
        return NULL;
    }
    strcpy(key, prefix);
    cbm_ht_set(hb->groups, key, group);
    return group;
}

static bool hu_group_push(hu_group_t *group, int entry_index) {
    if (group->count >= group->cap) {
        int want = group->cap > 0 ? group->cap * PAIR_LEN : CBM_SZ_8;
        int *grown = (int *)realloc(group->items, (size_t)want * sizeof(int));
        if (!grown) {
            return false;
        }
        group->items = grown;
        group->cap = want;
    }
    group->items[group->count++] = entry_index;
    return true;
}

static bool hu_build_push(hu_build_t *hb, hu_entry_t entry) {
    if (hb->count >= hb->cap) {
        int want = hb->cap > 0 ? hb->cap * PAIR_LEN : CBM_SZ_512;
        hu_entry_t *grown = (hu_entry_t *)realloc(hb->entries, (size_t)want * sizeof(*grown));
        if (!grown) {
            return false;
        }
        hb->entries = grown;
        hb->cap = want;
    }
    hb->entries[hb->count++] = entry;
    return true;
}

static void hu_collect_node(hu_ctx_t *hc, hu_build_t *hb, cbm_gbuf_node_t *node) {
    const char *label = node->label;
    const char *props = node->properties_json;
    bool is_method = label && (strcmp(label, "Method") == 0 || strcmp(label, "Constructor") == 0);
    bool is_type = label && (strcmp(label, "Class") == 0 || strcmp(label, "Interface") == 0 ||
                             strcmp(label, "Enum") == 0);
    if (!is_method && !is_type) {
        return;
    }
    if (!node->name || !node->qualified_name || !props) {
        hc->failed++;
        return;
    }

    int type_id = 0;
    const char *signature = "";
    bool no_body = false;
    char sig_buf[HU_INPUT_BUF];
    if (is_method) {
        bool has_body = false;
        if (!hu_get_bool(props, HU_PROP_HAS_BODY, &has_body)) {
            /* Not a Java method (the extractor writes hasBody for Java only) or
             * an extraction that predates the field. Either way there is no
             * TrackerV2 verdict to apply. */
            hc->skipped_not_tracker_type++;
            return;
        }
        /* The VALUE no longer gates identity. A declaration without a body
         * (interface method, abstract, native) is still a real, referencable
         * entity, so it gets a HashUID like any other member — see rule 1 at the
         * top of this file. Only the KEY's presence is consulted, because the
         * extractor writes `hasBody` for Java methods alone: that is what says
         * this node belongs to the tracked language at all. */
        no_body = !has_body;
        /* Constructor vs method comes from the extractor's own verdict, not
         * from `return_type`: an oversized properties blob may have dropped
         * fields from the tail, and "no return_type" is indistinguishable from
         * "return_type fell off the end". */
        char member_kind[CBM_SZ_32];
        bool is_ctor = hu_get_string(props, "\"javaMemberKind\":", member_kind,
                                     sizeof(member_kind)) &&
                       strcmp(member_kind, "constructor") == 0;
        type_id = is_ctor ? HU_TYPE_ID_CONSTRUCTOR : HU_TYPE_ID_METHOD;
        const char *sig = hu_canonical_signature(node, sig_buf, sizeof(sig_buf));
        if (!sig) {
            hc->failed++;
            return;
        }
        signature = sig;
    } else {
        /* A record or @interface has no TrackerV2 identity. */
        if (!hu_is_tracker_type(props)) {
            hc->skipped_not_tracker_type++;
            return;
        }
        if (strcmp(label, "Class") == 0) {
            type_id = HU_TYPE_ID_CLASS;
        } else if (strcmp(label, "Interface") == 0) {
            type_id = HU_TYPE_ID_INTERFACE;
        } else {
            type_id = HU_TYPE_ID_ENUM;
        }
    }

    char parent_class[CBM_SZ_512];
    parent_class[0] = '\0';
    (void)hu_get_string(props, "\"parent_class\":", parent_class, sizeof(parent_class));
    char parent_qn[CBM_SZ_512];
    hu_parent_qn(node, parent_class, parent_qn, sizeof(parent_qn));
    char module[CBM_SZ_2K];
    hu_logical_module(hc, parent_qn, module, sizeof(module));

    /* The seven-field prefix is the group key: TrackerV2 assigns the duplicate
     * fingerprint per GROUP of coinciding prefixes, so nothing may be stamped
     * until every candidate has been seen. */
    const char *path = node->file_path ? node->file_path : "";
    char prefix[HU_INPUT_BUF];
    int n = snprintf(prefix, sizeof(prefix), "%s|%s|%s|%s|%s|%d|%d|", node->name, signature, path,
                     "", module, type_id, HU_JAVA_LANGUAGE_TYPE_ID);
    if (n <= 0 || (size_t)n >= sizeof(prefix)) {
        hc->failed++;
        return;
    }

    char decl_hash[CBM_SZ_128];
    decl_hash[0] = '\0';
    if (!hu_get_string(props, "\"declHash\":", decl_hash, sizeof(decl_hash))) {
        hb->missing_hash++;
    }

    hu_group_t *group = hu_group_for(hb, prefix);
    if (!group) {
        hc->failed++;
        return;
    }
    hu_entry_t entry;
    memset(&entry, 0, sizeof(entry));
    entry.prefix = (char *)malloc(strlen(prefix) + SKIP_ONE);
    entry.module = (char *)malloc(strlen(module) + SKIP_ONE);
    entry.signature = (char *)malloc(strlen(signature) + SKIP_ONE);
    entry.decl_hash = decl_hash[0] ? (char *)malloc(strlen(decl_hash) + SKIP_ONE) : NULL;
    if (!entry.prefix || !entry.module || !entry.signature ||
        (decl_hash[0] && !entry.decl_hash)) {
        free(entry.prefix);
        free(entry.module);
        free(entry.signature);
        free((char *)entry.decl_hash);
        hc->failed++;
        return;
    }
    strcpy(entry.prefix, prefix);
    strcpy(entry.module, module);
    strcpy(entry.signature, signature);
    if (entry.decl_hash) {
        strcpy((char *)entry.decl_hash, decl_hash);
    }
    entry.node = node;
    entry.type_id = type_id;
    entry.no_body = no_body;
    entry.start_line = node->start_line;
    entry.end_line = node->end_line;
    entry.id = node->id;
    if (!hu_build_push(hb, entry)) {
        free(entry.prefix);
        free(entry.module);
        free(entry.signature);
        free((char *)entry.decl_hash);
        hc->failed++;
        return;
    }
    if (!hu_group_push(group, hb->count - SKIP_ONE)) {
        hc->failed++;
    }
}

/* ── stamping (phase B) ─────────────────────────────────────────────── */

/* TrackerV2's group order: (start_line, end_line), then extraction order. The
 * node id is assigned in creation order, which within one file is document
 * order — the same tie-break Python's stable sort produces. */
static int hu_entry_cmp(const void *lhs, const void *rhs) {
    const hu_entry_t *a = *(const hu_entry_t *const *)lhs;
    const hu_entry_t *b = *(const hu_entry_t *const *)rhs;
    if (a->start_line != b->start_line) {
        return a->start_line < b->start_line ? -1 : 1;
    }
    if (a->end_line != b->end_line) {
        return a->end_line < b->end_line ? -1 : 1;
    }
    if (a->id != b->id) {
        return a->id < b->id ? -1 : 1;
    }
    return 0;
}

static bool hu_stamp_entry(hu_ctx_t *hc, hu_entry_t *entry, const char *fingerprint) {
    char input[HU_INPUT_BUF];
    int n = snprintf(input, sizeof(input), "%s%s", entry->prefix, fingerprint);
    if (n <= 0 || (size_t)n >= sizeof(input)) {
        return false;
    }
    char hex[CBM_MD5_HEX_LEN + SKIP_ONE];
    cbm_md5_hex(input, strlen(input), hex);

    /* The full input, the canonical signature and the fingerprint are kept
     * beside the digest: they are what make a mismatch against TrackerV2
     * diagnosable field by field instead of a 32-char dead end. Every write is
     * attempted — a failure on one must not silently drop the others, or a node
     * would end up half-identified and blame-free. */
    bool ok = true;
    ok &= hu_node_set_string(entry->node, "hashInput", input);
    ok &= hu_node_set_string(entry->node, "hashuid", hex);
    ok &= hu_node_set_string(entry->node, "canonicalSignature", entry->signature);
    ok &= hu_node_set_string(entry->node, "logicalModule", entry->module);
    ok &= hu_node_set_string(entry->node, "duplicateFingerprint", fingerprint);
    ok &= hu_node_set_int(entry->node, "typeId", entry->type_id);
    ok &= hu_node_set_int(entry->node, "languageTypeId", HU_JAVA_LANGUAGE_TYPE_ID);
    return ok;
}

static void hu_stamp_group(const char *key, void *value, void *ud) {
    (void)key;
    hu_ctx_t *hc = (hu_ctx_t *)ud;
    hu_group_t *group = (hu_group_t *)value;
    hu_build_t *hb = (hu_build_t *)hc->build;
    if (!group || !hb || group->count <= 0) {
        return;
    }
    const hu_entry_t **members =
        (const hu_entry_t **)malloc((size_t)group->count * sizeof(*members));
    if (!members) {
        hc->failed += (uint64_t)group->count;
        return;
    }
    for (int i = 0; i < group->count; i++) {
        members[i] = &hb->entries[group->items[i]];
    }
    if (group->count > SKIP_ONE) {
        qsort(members, (size_t)group->count, sizeof(*members), hu_entry_cmp);
    }

    for (int i = 0; i < group->count; i++) {
        char fingerprint[CBM_SZ_128];
        fingerprint[0] = '\0';
        if (group->count > SKIP_ONE) {
            if (members[i]->decl_hash) {
                int n = snprintf(fingerprint, sizeof(fingerprint), "%s%d", members[i]->decl_hash, i);
                if (n <= 0 || (size_t)n >= sizeof(fingerprint)) {
                    fingerprint[0] = '\0';
                }
            } else {
                /* No digest (an older DB, or a path that did not record one).
                 * The index still separates the members so the unique index
                 * cannot reject the build, and the '?' marker says this value is
                 * NOT TrackerV2-compatible instead of looking silently right. */
                snprintf(fingerprint, sizeof(fingerprint), "?%lld", (long long)members[i]->id);
            }
        }
        if (!hu_stamp_entry(hc, (hu_entry_t *)members[i], fingerprint)) {
            hc->failed++;
        } else {
            hc->stamped++;
            if (members[i]->no_body) {
                hc->stamped_no_body++;
            }
        }
    }
    free(members);
}

/* Release one group: its item array. The struct and its key are freed by the
 * same walk that owns them. */
static void hu_free_group(const char *key, void *value, void *ud) {
    (void)ud;
    free((char *)key);
    hu_group_t *group = (hu_group_t *)value;
    if (group) {
        free(group->items);
        free(group);
    }
}

/* ── entry point ─────────────────────────────────────────────────── */

static bool hu_label_is_container(const char *label) {
    return label && (strcmp(label, "Interface") == 0 || strcmp(label, "Enum") == 0);
}

static void hu_collect_container(const cbm_gbuf_node_t *node, CBMHashTable *containers) {
    if (!node || !node->qualified_name || !node->name || !node->label) {
        return;
    }
    bool is_type = strcmp(node->label, "Class") == 0 || hu_label_is_container(node->label);
    if (!is_type || !hu_scopes_members(node->properties_json)) {
        return;
    }
    cbm_ht_set(containers, node->qualified_name, (void *)node->name);
}

static void hu_free_key(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    free((char *)key);
}

/* Record a callable's SIGNATURE-FREE qn as a scope prefix. The scope chain
 * names a method without its parameter list, so `...Probe.outer(Sink)` has to
 * be reachable under `...Probe.outer`. The key is owned here (the gbuf's own
 * string is longer than the prefix) and released with the table. */
static void hu_index_member_scope(const cbm_gbuf_node_t *node, CBMHashTable *table,
                                  const char *signature_less_qn) {
    if (!node || !signature_less_qn || !table) {
        return;
    }
    void *existing = cbm_ht_get(table, signature_less_qn);
    if (existing) {
        return; /* overloads share a prefix; one entry answers for all */
    }
    size_t len = strlen(signature_less_qn);
    char *key = (char *)malloc(len + SKIP_ONE);
    if (!key) {
        return;
    }
    memcpy(key, signature_less_qn, len + SKIP_ONE);
    cbm_ht_set(table, key, (void *)node);
}

void cbm_pipeline_pass_hashuid(cbm_pipeline_ctx_t *ctx) {
    if (!ctx || !ctx->gbuf) {
        return;
    }
    cbm_gbuf_t *gb = ctx->gbuf;

    hu_ctx_t hc;
    memset(&hc, 0, sizeof(hc));
    hc.containers = cbm_ht_create(CBM_SZ_512);
    hc.member_scopes = cbm_ht_create(CBM_SZ_512);
    if (!hc.containers || !hc.member_scopes) {
        cbm_ht_free(hc.containers);
        cbm_ht_free(hc.member_scopes);
        cbm_log_error("pass.hashuid", "msg", "container_index_alloc_failed");
        return;
    }

    /* Two phases: every container first, because a member's logical_module can
     * name a class declared in a file processed arbitrarily later. */
    static const char *const labels[] = {"Class", "Interface", "Enum"};
    for (size_t li = 0; li < sizeof(labels) / sizeof(labels[0]); li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[li], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            hu_collect_container(nodes[i], hc.containers);
        }
    }

    /* Callables are scopes too (TrackerV2 counts method/constructor
     * declarations as containers) — that is what puts the enclosing method into
     * the module of an anonymous class's members. */
    static const char *const callable_labels[] = {"Function", "Method", "Constructor"};
    for (size_t li = 0; li < sizeof(callable_labels) / sizeof(callable_labels[0]); li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, callable_labels[li], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            const cbm_gbuf_node_t *n = nodes[i];
            if (!n || !n->qualified_name) {
                continue;
            }
            const char *open = strchr(n->qualified_name, '(');
            if (!open) {
                continue; /* a plain name cannot be a stripped prefix of itself */
            }
            char prefix[CBM_SZ_2K];
            size_t len = (size_t)(open - n->qualified_name);
            if (len == 0 || len >= sizeof(prefix)) {
                continue;
            }
            memcpy(prefix, n->qualified_name, len);
            prefix[len] = '\0';
            hu_index_member_scope(n, hc.member_scopes, prefix);
        }
    }

    hu_build_t hb;
    memset(&hb, 0, sizeof(hb));
    hb.groups = cbm_ht_create(CBM_SZ_512);
    if (!hb.groups) {
        cbm_ht_foreach(hc.member_scopes, hu_free_key, NULL);
        cbm_ht_free(hc.member_scopes);
        cbm_ht_free(hc.containers);
        cbm_log_error("pass.hashuid", "msg", "group_index_alloc_failed");
        return;
    }

    /* Phase A: collect every candidate under its seven-field prefix. */
    static const char *const member_labels[] = {"Method", "Class", "Interface", "Enum"};
    for (size_t li = 0; li < sizeof(member_labels) / sizeof(member_labels[0]); li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, member_labels[li], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            hu_collect_node(&hc, &hb, (cbm_gbuf_node_t *)nodes[i]);
        }
    }

    /* Phase B: stamp each group, members in TrackerV2's order. */
    hc.build = (struct hu_build_s *)&hb;
    cbm_ht_foreach(hb.groups, hu_stamp_group, &hc);
    hc.build = NULL;

    for (int i = 0; i < hb.count; i++) {
        free(hb.entries[i].prefix);
        free(hb.entries[i].module);
        free(hb.entries[i].signature);
        free((char *)hb.entries[i].decl_hash);
    }
    free(hb.entries);
    cbm_ht_foreach(hb.groups, hu_free_group, NULL);
    cbm_ht_free(hb.groups);

    cbm_ht_foreach(hc.member_scopes, hu_free_key, NULL);
    cbm_ht_free(hc.member_scopes);
    cbm_ht_free(hc.containers);

    cbm_log_info("pass.done", "pass", "hashuid", "stamped", hu_u64(hc.stamped), "no_body",
                 hu_u64(hc.stamped_no_body), "skipped_type", hu_u64(hc.skipped_not_tracker_type),
                 "failed", hu_u64(hc.failed), "groups", hu_u64((uint64_t)hb.count),
                 "no_decl_hash", hu_u64(hb.missing_hash));
}
