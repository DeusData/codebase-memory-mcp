/*
 * doc_links_msbuild.c — MSBuild global usings of a C# project (R1).
 *
 * A reader of data, never a build: nothing is executed, fetched or opened.
 * The input is the scope blob the extractor writes for every MSBuild project
 * file the index holds (internal/cbm/doclink_cs.c, "Project blob"). A file
 * discovery did not take -- a symbolic link, a path outside the repository,
 * an ignored file -- does not exist here.
 *
 * Evaluation model (MSBuild's passes, reduced):
 *   files       the nearest Directory.Build.props above the project, the
 *               project file, the nearest Directory.Build.targets; an
 *               <Import> is evaluated where it stands, every file once
 *   pass 1      properties in evaluation order, later wins
 *   pass 2      <Using Include|Remove> items with the final properties;
 *               Static="true" names a type, Alias an alias; a Remove takes a
 *               target out wherever it was included
 *   implicit    ImplicitUsings enable/true adds the default set of the
 *               project's SDK (Microsoft.NET.Sdk / .Web / .Worker)
 *
 * Nothing is guessed. A condition has three values: true, false, and UNKNOWN
 * for what this reader cannot decide -- a property no evaluated file sets
 * (the SDK, the environment or the command line may set it), a property
 * function, an item or metadata reference, a relational operator, a function
 * call, a comparison whose outcome depends on how MSBuild converts its
 * operands. An element under an unknown condition is not applied and is
 * counted. What it could have set is unknown from there on: its properties
 * poison the conditions that read them, and a <Using> or an ImplicitUsings
 * switch that cannot be evaluated leaves the project's usings open
 * (cbm_msb_result_t.open).
 *
 * Paths: $(MSBuildThisFileDirectory) and $(MSBuildProjectDirectory) are
 * absolute in MSBuild. Here they start with a mark byte followed by the
 * repository-relative path, so an import through them is not joined to the
 * importing file's directory a second time, and a path the file writes
 * absolute itself (/usr/..., C:\...) stays what it is: outside.
 */
#include "pipeline/doc_links_msbuild.h"

#include "doclink.h" /* CBM_DOCLINK_CS_SCOPE_TAG */
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"

#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    MSB_FIELDS = 5,
    MSB_NAME_MAX = 256,   /* a property name; a longer one never has a known value */
    MSB_VALUE_MAX = 4096, /* an expanded value; a longer one is unknown */
    MSB_COND_DEPTH = 32,  /* parentheses of a condition; deeper is unknown */
    MSB_SIGNIFICANT = 15, /* the decimal digits a double holds exactly */
    MSB_PATH_MARK = 1,    /* first byte of a repository-absolute path */
    MSB_INIT = 16,
};

typedef enum { MSB_FALSE = 0, MSB_TRUE = 1, MSB_UNKNOWN = 2 } msb_tri_t;

/* ── The project files ───────────────────────────────────────────── */

typedef struct {
    char tag;                  /* I G V W H N K Y C; W is a V whose value is no plain text */
    const char *f[MSB_FIELDS]; /* NULL: the attribute is not there */
} msb_rec_t;

typedef struct {
    const char *rel_path;
    const char *dir;      /* "" for the repository root */
    const char *name;     /* the file name */
    const char *stem;     /* ... without its extension */
    const char *ext;      /* ".csproj"; "" when it has none */
    const char *abs_dir;  /* marked, with a trailing '/' */
    const char *abs_path; /* marked */
    const char *sdk;      /* the <Project Sdk> attribute, or NULL */
    bool readable;
    msb_rec_t *recs;
    int nrecs;
} msb_file_t;

struct cbm_msb {
    CBMArena arena;
    msb_file_t *files;
    int nfiles;
    int cap;
    CBMHashTable *by_path; /* rel_path -> index + 1 */
    uint64_t generation;   /* includes attempted additions that may partially mutate storage */
    bool oom;
};

bool cbm_msb_is_project_scope(const char *scope) {
    static const char head[] = CBM_DOCLINK_CS_SCOPE_TAG "\nP\t";
    return scope && strncmp(scope, head, sizeof(head) - SKIP_ONE) == 0;
}

cbm_msb_t *cbm_msb_new(void) {
    cbm_msb_t *m = (cbm_msb_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
    if (!m) {
        return NULL;
    }
    cbm_arena_init(&m->arena);
    m->by_path = cbm_ht_create(CBM_SZ_256);
    if (!m->by_path) {
        cbm_msb_free(m);
        return NULL;
    }
    return m;
}

void cbm_msb_free(cbm_msb_t *m) {
    if (!m) {
        return;
    }
    cbm_ht_free(m->by_path);
    cbm_arena_destroy(&m->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, m);
}

static void *msb_alloc(cbm_msb_t *m, size_t n) {
    void *p = cbm_arena_alloc(&m->arena, n ? n : SKIP_ONE);
    m->oom = m->oom || !p;
    return p;
}

/* a + b + c, in the arena. */
static char *msb_join3(cbm_msb_t *m, const char *a, const char *b, const char *c) {
    size_t al = strlen(a);
    size_t bl = strlen(b);
    size_t cl = strlen(c);
    char *out = (char *)msb_alloc(m, al + bl + cl + SKIP_ONE);
    if (out) {
        memcpy(out, a, al);
        memcpy(out + al, b, bl);
        memcpy(out + al + bl, c, cl + SKIP_ONE);
    }
    return out;
}

/* A blob field in place: NULL for an absent attribute, else the text after
 * the '=' with its escapes resolved. */
static char *msb_field(char *raw) {
    if (!raw || raw[0] != '=') {
        return NULL;
    }
    char *out = raw + SKIP_ONE;
    char *w = out;
    for (const char *r = out; *r; r++) {
        if (*r == '\\' && r[1]) {
            r++;
            *w++ = *r == 't' ? '\t' : (*r == 'n' ? '\n' : (*r == 'r' ? '\r' : *r));
        } else {
            *w++ = *r;
        }
    }
    *w = '\0';
    return out;
}

/* Cut a line (NUL-terminated, its tag in line[0]) into its fields in place. */
static void msb_split(char *line, char *raw[MSB_FIELDS]) {
    int n = 0;
    for (int i = 0; i < MSB_FIELDS; i++) {
        raw[i] = NULL;
    }
    for (char *p = line; *p && n < MSB_FIELDS; p++) {
        if (*p == '\t') {
            *p = '\0';
            raw[n++] = p + SKIP_ONE;
        }
    }
}

/* One line of the blob as a record. */
static void msb_parse_record(char *line, msb_rec_t *r) {
    char *raw[MSB_FIELDS];
    msb_split(line, raw);
    memset(r, 0, sizeof(*r));
    r->tag = line[0];
    if (r->tag == 'V') {
        r->f[0] = msb_field(raw[0]);
        r->f[1] = raw[1] ? raw[1] : "";
        if (raw[2] && raw[2][0] == '=') {
            r->f[2] = msb_field(raw[2]);
        } else {
            r->tag = 'W';
        }
    } else if (r->tag == 'K' || r->tag == 'C') {
        r->f[0] = raw[0] ? raw[0] : "";
    } else {
        for (int i = 0; i < MSB_FIELDS; i++) {
            r->f[i] = msb_field(raw[i]);
        }
    }
}

/* Fill the names derived from the file's path. */
static void msb_file_names(cbm_msb_t *m, msb_file_t *f) {
    const char *slash = strrchr(f->rel_path, '/');
    f->name = slash ? slash + SKIP_ONE : f->rel_path;
    char *dir =
        cbm_arena_strndup(&m->arena, f->rel_path, slash ? (size_t)(slash - f->rel_path) : 0);
    const char *dot = strrchr(f->name, '.');
    char *stem =
        cbm_arena_strndup(&m->arena, f->name, dot ? (size_t)(dot - f->name) : strlen(f->name));
    m->oom = m->oom || !dir || !stem;
    f->dir = dir ? dir : "";
    f->stem = stem ? stem : "";
    f->ext = dot ? dot : "";
    static const char mark[] = {MSB_PATH_MARK, '/', '\0'};
    f->abs_dir = msb_join3(m, mark, f->dir, f->dir[0] ? "/" : "");
    f->abs_path = msb_join3(m, mark, f->rel_path, "");
}

bool cbm_msb_add(cbm_msb_t *m, const char *rel_path, const char *scope) {
    if (!m || m->oom || !rel_path) {
        return false;
    }
    if (!cbm_msb_is_project_scope(scope) || cbm_ht_get(m->by_path, rel_path)) {
        return true; /* no project file, or one that is there already */
    }
    /* Invalidate derived state before any allocation or partial mutation. */
    m->generation++;
    if (m->nfiles >= m->cap) {
        int ncap = m->cap ? m->cap * PAIR_LEN : CBM_SZ_64;
        msb_file_t *grown = (msb_file_t *)msb_alloc(m, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        if (m->nfiles > 0) {
            memcpy(grown, m->files, (size_t)m->nfiles * sizeof(*grown));
        }
        m->files = grown;
        m->cap = ncap;
    }
    msb_file_t *f = &m->files[m->nfiles];
    memset(f, 0, sizeof(*f));
    f->rel_path = cbm_arena_strdup(&m->arena, rel_path);
    char *buf = cbm_arena_strdup(&m->arena, scope);
    if (!f->rel_path || !buf) {
        m->oom = true;
        return false;
    }
    msb_file_names(m, f);
    int lines = 0;
    for (const char *p = buf; *p; p++) {
        lines += *p == '\n';
    }
    f->recs = (msb_rec_t *)msb_alloc(m, (size_t)(lines + SKIP_ONE) * sizeof(msb_rec_t));
    if (m->oom) {
        return false;
    }
    /* line 0 is the tag, line 1 the P record */
    int line_no = 0;
    bool import_group = false;
    bool bad_group = false;
    const char *group_cond = NULL;
    for (char *line = buf; line && *line; line_no++) {
        char *nl = strchr(line, '\n');
        if (nl) {
            *nl = '\0';
        }
        if (line_no == SKIP_ONE) {
            char *raw[MSB_FIELDS];
            msb_split(line, raw); /* P  sdk  state */
            f->sdk = msb_field(raw[0]);
            f->readable = raw[1] && raw[1][0] == '-';
        } else if (line_no > SKIP_ONE) {
            msb_rec_t *r = &f->recs[f->nrecs];
            msb_parse_record(line, r);
            if (import_group && r->tag != 'J' && r->tag != 'E') {
                bad_group = true;
            }
            if (r->tag == 'B') {
                bad_group = bad_group || line[1] || r->f[1] || r->f[2] || r->f[3] || r->f[4];
                import_group = true;
                group_cond = r->f[0];
            } else if (r->tag == 'E') {
                bad_group = bad_group || !import_group || line[1] || r->f[0] || r->f[1] ||
                            r->f[2] || r->f[3] || r->f[4];
                import_group = false;
                group_cond = NULL;
            } else {
                if (r->tag == 'J') {
                    bad_group = bad_group || !import_group || line[1] || r->f[0] || r->f[4];
                    /* The decoded text lives in buf, not in the reused record.
                     * Each import still evaluates it at its original position. */
                    r->tag = 'I';
                    r->f[0] = group_cond;
                }
                f->nrecs++;
            }
            if (bad_group) {
                break;
            }
        }
        line = nl ? nl + SKIP_ONE : NULL;
    }
    if (bad_group || import_group || !f->readable) {
        /* A malformed group is unknown, never an empty closed scope. Keep
         * this project conservative without rejecting other project files.
         * So is a file the scan did not read (malformed, or larger than a
         * project file): what it holds is unknown, not absent -- the
         * projects that evaluate it have an open scope. */
        f->readable = false;
        f->recs[0] = (msb_rec_t){.tag = 'Y'};
        f->nrecs = 1;
    }
    cbm_ht_set(m->by_path, f->rel_path, (void *)(intptr_t)(m->nfiles + SKIP_ONE));
    m->nfiles++;
    return !m->oom;
}

static void msb_work(uint64_t n);

static int msb_file_index(const cbm_msb_t *m, const char *rel_path) {
    msb_work(SKIP_ONE); /* one logical path lookup, not hash-table bucket probes */
    intptr_t v = (intptr_t)cbm_ht_get(m->by_path, rel_path);
    return v > 0 ? (int)(v - SKIP_ONE) : CBM_NOT_FOUND;
}

bool cbm_msb_has(const cbm_msb_t *m, const char *rel_path) {
    return m && rel_path && msb_file_index(m, rel_path) >= 0;
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
#include <stdatomic.h>
static _Atomic uint64_t msb_record_counter;
static _Atomic uint64_t msb_work_counter;
static _Atomic uint64_t msb_nearest_counter;
static _Atomic uint64_t msb_peak_counter;
static _Atomic int msb_value_alloc_after;
static _Atomic bool msb_value_alloc_failed;
static _Atomic int msb_item_fail_operation;
static _Atomic int msb_item_fail_after;
static _Atomic bool msb_item_operation_failed;
static _Atomic int msb_node_fail_operation;
static _Atomic int msb_node_fail_after;
static _Atomic bool msb_node_alloc_failed;
static _Atomic uint64_t msb_state_allocations;
static _Atomic uint64_t msb_state_slot_reuses;
static _Atomic uint64_t msb_state_witnessed_slot_reuses;
static _Atomic uint64_t msb_state_same_length_value_changes;
static _Atomic uint64_t msb_state_witness_skips;
static _Atomic uint64_t msb_state_revision_errors;

void cbm_msb_test_fail_node_alloc(cbm_msb_node_fail_operation_t operation, int nth) {
    atomic_store(&msb_node_fail_operation, (int)operation);
    atomic_store(&msb_node_fail_after, nth);
    atomic_store(&msb_node_alloc_failed, false);
}

bool cbm_msb_test_node_alloc_failed(void) {
    return atomic_load(&msb_node_alloc_failed);
}

void cbm_msb_test_state_stats(cbm_msb_state_test_stats_t *out) {
    out->allocations = atomic_load(&msb_state_allocations);
    out->slot_reuses = atomic_load(&msb_state_slot_reuses);
    out->witnessed_slot_reuses = atomic_load(&msb_state_witnessed_slot_reuses);
    out->same_length_value_changes = atomic_load(&msb_state_same_length_value_changes);
    out->witness_skips = atomic_load(&msb_state_witness_skips);
    out->revision_errors = atomic_load(&msb_state_revision_errors);
}

void cbm_msb_test_fail_item_operation(cbm_msb_item_fail_operation_t operation, int nth) {
    atomic_store(&msb_item_fail_operation, (int)operation);
    atomic_store(&msb_item_fail_after, nth);
    atomic_store(&msb_item_operation_failed, false);
}

bool cbm_msb_test_item_operation_failed(void) {
    return atomic_load(&msb_item_operation_failed);
}

static bool msb_fail_item_operation(cbm_msb_item_fail_operation_t operation) {
    if (operation == CBM_MSB_ITEM_FAIL_NONE ||
        atomic_load(&msb_item_fail_operation) != (int)operation) {
        return false;
    }
    int n = atomic_load(&msb_item_fail_after);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&msb_item_fail_after, &n, n - SKIP_ONE)) {
            if (n == SKIP_ONE) {
                atomic_store(&msb_item_operation_failed, true);
                return true;
            }
            return false;
        }
    }
    return false;
}

void cbm_msb_test_fail_value_alloc_after(int nth) {
    atomic_store(&msb_value_alloc_after, nth);
    atomic_store(&msb_value_alloc_failed, false);
}

bool cbm_msb_test_value_alloc_failed(void) {
    return atomic_load(&msb_value_alloc_failed);
}

static bool msb_fail_value_alloc(void) {
    int n = atomic_load(&msb_value_alloc_after);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&msb_value_alloc_after, &n, n - SKIP_ONE)) {
            if (n == SKIP_ONE) {
                atomic_store(&msb_value_alloc_failed, true);
                return true;
            }
            return false;
        }
    }
    return false;
}

static _Atomic int msb_prop_insert_after;
static _Atomic bool msb_prop_insert_failed;
static _Atomic uint64_t msb_value_live_counter;

void cbm_msb_test_fail_prop_insert_after(int nth) {
    atomic_store(&msb_prop_insert_after, nth);
    atomic_store(&msb_prop_insert_failed, false);
}

bool cbm_msb_test_prop_insert_failed(void) {
    return atomic_load(&msb_prop_insert_failed);
}

uint64_t cbm_msb_test_value_live_bytes(void) {
    return atomic_load(&msb_value_live_counter);
}

static bool msb_fail_prop_insert(void) {
    int n = atomic_load(&msb_prop_insert_after);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&msb_prop_insert_after, &n, n - SKIP_ONE)) {
            if (n == SKIP_ONE) {
                atomic_store(&msb_prop_insert_failed, true);
                return true;
            }
            return false;
        }
    }
    return false;
}

void cbm_msb_test_cost_reset(void) {
    atomic_store(&msb_record_counter, 0);
    atomic_store(&msb_work_counter, 0);
    atomic_store(&msb_nearest_counter, 0);
    atomic_store(&msb_peak_counter, 0);
    atomic_store(&msb_state_allocations, 0);
    atomic_store(&msb_state_slot_reuses, 0);
    atomic_store(&msb_state_witnessed_slot_reuses, 0);
    atomic_store(&msb_state_same_length_value_changes, 0);
    atomic_store(&msb_state_witness_skips, 0);
    atomic_store(&msb_state_revision_errors, 0);
}

void cbm_msb_test_cost(uint64_t *records, uint64_t *peak_bytes) {
    *records = atomic_load(&msb_record_counter);
    *peak_bytes = atomic_load(&msb_peak_counter);
}

uint64_t cbm_msb_test_work(void) {
    return atomic_load(&msb_work_counter);
}

uint64_t cbm_msb_test_nearest_steps(void) {
    return atomic_load(&msb_nearest_counter);
}

static void msb_nearest_step(void) {
    atomic_fetch_add_explicit(&msb_nearest_counter, SKIP_ONE, memory_order_relaxed);
}

static void msb_work(uint64_t n) {
    atomic_fetch_add_explicit(&msb_work_counter, n, memory_order_relaxed);
}

static void msb_record(void) {
    atomic_fetch_add_explicit(&msb_record_counter, SKIP_ONE, memory_order_relaxed);
    msb_work(SKIP_ONE);
}

static void msb_peak(uint64_t bytes) {
    uint64_t old = atomic_load(&msb_peak_counter);
    while (old < bytes && !atomic_compare_exchange_weak(&msb_peak_counter, &old, bytes)) {}
}
#else
static void msb_nearest_step(void) {}
static void msb_work(uint64_t n) {
    (void)n;
}
static void msb_record(void) {}
static bool msb_fail_item_operation(cbm_msb_item_fail_operation_t operation) {
    (void)operation;
    return false;
}
#endif

static bool msb_fail_node_alloc(cbm_msb_node_fail_operation_t operation) {
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (operation == CBM_MSB_NODE_FAIL_NONE ||
        atomic_load(&msb_node_fail_operation) != (int)operation)
        return false;
    int n = atomic_load(&msb_node_fail_after);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&msb_node_fail_after, &n, n - 1)) {
            if (n == 1) {
                atomic_store(&msb_node_alloc_failed, true);
                return true;
            }
            return false;
        }
    }
#else
    (void)operation;
#endif
    return false;
}

typedef struct st_node st_node_t;
typedef struct st_rope st_rope_t;
typedef struct st_component st_component_t;
typedef struct msb_state msb_state_t;
typedef struct {
    st_node_t *root[3];
} msb_view_t;

/* ── One evaluation ──────────────────────────────────────────────── */

typedef struct {
    char *value; /* owned current value; NULL: set, but to nothing this reader knows */
    size_t capacity;
} msb_prop_t;

typedef struct {
    const msb_rec_t *group; /* the <ItemGroup>'s H record */
    const msb_rec_t *item;  /* the <Using>'s N record */
    int file;
} msb_item_t;

typedef struct {
    int file;
    int rec;
    st_component_t *owner;
    msb_tri_t group;         /* the condition of the <PropertyGroup> being read */
    const msb_rec_t *hgroup; /* the <ItemGroup> being read */
    /* The <ImportGroup> being read: its condition's text (one string the
     * group's imports share) and what it came to. MSBuild evaluates a
     * group's condition once, where the group stands; evaluating it again
     * for every import cost imports x condition length. */
    const char *igroup_text;
    msb_tri_t igroup;
} msb_frame_t;

enum { MSB_SEEDS = 5, MSB_LIVE_OWNERS = 5 };
static const char *const MSB_SEED_NAMES[MSB_SEEDS] = {
    "msbuildprojectname", "msbuildprojectfile", "msbuildprojectextension", "msbuildprojectfullpath",
    "msbuildprojectdirectory"};

typedef struct {
    bool present; /* absent and present-unknown are different dependencies */
    const char *value;
    size_t bytes;
    bool exception; /* expected state differs from the immutable input layers */
} msb_prop_dep_t;

typedef struct {
    bool present;
    bool exception;
} msb_set_dep_t;

typedef struct {
    CBMHashTable *props;
    CBMHashTable *seen;
    CBMHashTable *poisoned;
    msb_prop_dep_t *seeds[MSB_SEEDS];
    size_t prop_exceptions;
    size_t seen_exceptions;
    size_t poisoned_exceptions;
} msb_inputs_t;

typedef struct msb_eval msb_eval_t;
typedef struct {
    const msb_eval_t *owners[MSB_LIVE_OWNERS]; /* prefix, target, project, seeds, items */
    const size_t *metadata_bytes; /* live directory-memo payloads, including pending insertion */
    const size_t *state_bytes;
    const CBMArena *state_names;
} msb_live_t;

struct msb_eval {
    const cbm_msb_t *m;
    msb_state_t *state;
    msb_view_t view, state_inputs;
    st_node_t *reuse_nodes; /* operation-local identity reuse, never a lookup layer */
    st_component_t *builder, *completed;
    int main_project;
    const msb_eval_t *base;    /* immutable completed prefix, never written through */
    const msb_eval_t *seeds;   /* this project's five initial properties */
    const msb_eval_t *effects; /* completed target writes, highest precedence in pass 2 */
    msb_eval_t *incoming;      /* read-only project input while capturing effects or final items */
    const msb_live_t *live;    /* simultaneous owners for peak accounting */
    msb_inputs_t inputs;
    CBMHashTable *shared_removals; /* borrowed exact item result, only during publication */
    bool capture_inputs;
    cbm_msb_item_fail_operation_t item_alloc_operation;
    bool track_seed_reads;
    bool seed_read;
    CBMArena arena;         /* keys, property owners, frames, items and output copies */
    CBMArena scratch;       /* expressions and paths, released after their consumers finish */
    CBMHashTable *props;    /* lower-cased name -> msb_prop_t* */
    CBMHashTable *seen;     /* rel_path: files evaluated */
    CBMHashTable *poisoned; /* rel_path: files whose properties were made unknown */
    msb_item_t *items;
    int nitems;
    int cap_items;
    msb_frame_t *frames;
    int nframes;
    int cap_frames;
    bool open;
    int unevaluable;
    int outside;
    size_t result_bytes;
    size_t value_bytes; /* live property buffers, including a replacement before publication */
    bool oom;
};

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static size_t ev_owned_bytes(const msb_eval_t *ev) {
    return ev ? cbm_arena_capacity(&ev->arena) + cbm_arena_capacity(&ev->scratch) +
                    ev->value_bytes + ev->result_bytes
              : 0;
}
#endif

static void ev_peak(const msb_eval_t *ev) {
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    size_t bytes = ev_owned_bytes(ev);
    if (ev->live) {
        bytes = ev->live->metadata_bytes ? *ev->live->metadata_bytes : 0;
        if (ev->live->state_bytes)
            bytes += *ev->live->state_bytes;
        if (ev->live->state_names)
            bytes += cbm_arena_capacity(ev->live->state_names);
        for (int i = 0; i < MSB_LIVE_OWNERS; i++) {
            bytes += ev_owned_bytes(ev->live->owners[i]);
        }
    }
    msb_peak(bytes);
#else
    (void)ev;
#endif
}

static void *ev_alloc(msb_eval_t *ev, size_t n) {
    void *p = msb_fail_item_operation(ev->item_alloc_operation)
                  ? NULL
                  : cbm_arena_alloc(&ev->arena, n ? n : SKIP_ONE);
    ev->oom = ev->oom || !p;
    ev_peak(ev);
    return p;
}

static char *ev_strndup(msb_eval_t *ev, const char *s, size_t n) {
    char *p = msb_fail_item_operation(ev->item_alloc_operation)
                  ? NULL
                  : cbm_arena_strndup(&ev->arena, s, n);
    if (p) {
        msb_work(n + SKIP_ONE);
    }
    ev->oom = ev->oom || !p;
    ev_peak(ev);
    return p;
}

static void *scratch_alloc(msb_eval_t *ev, size_t n) {
    void *p = msb_fail_item_operation(ev->item_alloc_operation)
                  ? NULL
                  : cbm_arena_alloc(&ev->scratch, n ? n : SKIP_ONE);
    ev->oom = ev->oom || !p;
    ev_peak(ev);
    return p;
}

static char *scratch_strndup(msb_eval_t *ev, const char *s, size_t n) {
    char *p = msb_fail_item_operation(ev->item_alloc_operation)
                  ? NULL
                  : cbm_arena_strndup(&ev->scratch, s, n);
    if (p) {
        msb_work(n + SKIP_ONE);
    }
    ev->oom = ev->oom || !p;
    ev_peak(ev);
    return p;
}

/* Every property buffer is owned by a table entry. Account for the new and
 * old buffers simultaneously until copying has finished and the old one is
 * released; neither a replacement nor an unknown value retains history. */
static char *value_alloc(msb_eval_t *ev, size_t n) {
    bool fail = n > SIZE_MAX - ev->value_bytes;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    fail = fail || msb_fail_value_alloc();
#endif
    if (fail) {
        ev->oom = true;
        return NULL;
    }
    char *p = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n);
    if (!p) {
        ev->oom = true;
        return NULL;
    }
    ev->value_bytes += n;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    atomic_fetch_add_explicit(&msb_value_live_counter, n, memory_order_relaxed);
#endif
    ev_peak(ev);
    return p;
}

static void value_clear(msb_eval_t *ev, msb_prop_t *p) {
    if (p->value) {
        cbm_free(CBM_MEM_CLASS_OTHER, p->value);
        ev->value_bytes -= p->capacity;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        atomic_fetch_sub_explicit(&msb_value_live_counter, p->capacity, memory_order_relaxed);
#endif
    }
    p->value = NULL;
    p->capacity = 0;
}

static void prop_clear(const char *key, void *value, void *userdata) {
    (void)key;
    msb_work(SKIP_ONE); /* every property owner visited during teardown */
    value_clear((msb_eval_t *)userdata, (msb_prop_t *)value);
}

/* The table key of a property name; false for a name too long to keep. */
static bool msb_key(const char *name, size_t len, char key[MSB_NAME_MAX]) {
    if (len == 0 || len >= MSB_NAME_MAX) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        key[i] = (char)tolower((unsigned char)name[i]);
    }
    key[len] = '\0';
    msb_work(len + SKIP_ONE); /* lower-casing and the terminator */
    return true;
}

static void st_set_property(msb_eval_t *ev, const char *name, const char *value);
static const msb_prop_t *st_property(msb_eval_t *ev, const char *key);

/* Set a property; value NULL makes it unknown. */
static void msb_set(msb_eval_t *ev, const char *name, const char *value) {
    if (ev->state) {
        st_set_property(ev, name, value);
        return;
    }
    if (ev->oom) {
        return;
    }
    char key[MSB_NAME_MAX];
    if (!msb_key(name, strlen(name), key)) {
        return;
    }
    msb_work(SKIP_ONE);
    msb_prop_t *p = (msb_prop_t *)cbm_ht_get(ev->props, key);
    if (!p) {
        p = (msb_prop_t *)ev_alloc(ev, sizeof(*p));
        if (!p) {
            return;
        }
        memset(p, 0, sizeof(*p));
        char *k = ev_strndup(ev, key, strlen(key));
        if (!k) {
            return;
        }
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        if (!msb_fail_prop_insert())
#endif
        {
            msb_work(SKIP_ONE);
            cbm_ht_set(ev->props, k, p);
        }
        /* set returns the old value, so NULL alone cannot prove insertion.
         * Acquire no heap buffer until cleanup can reach this owner. */
        msb_work(SKIP_ONE);
        if (cbm_ht_get(ev->props, k) != p) {
            ev->oom = true;
            return;
        }
    }
    if (!value) {
        value_clear(ev, p);
        return;
    }
    size_t bytes = strlen(value) + SKIP_ONE;
    if (p->capacity == bytes) {
        memmove(p->value, value, bytes);
        msb_work(bytes);
        return;
    }
    char *replacement = value_alloc(ev, bytes);
    if (!replacement) {
        return; /* sticky OOM prevents publishing a partial evaluation */
    }
    memcpy(replacement, value, bytes);
    msb_work(bytes);
    value_clear(ev, p);
    p->value = replacement;
    p->capacity = bytes;
}

/* Compare exact input state. Known-empty is a one-byte owned value;
 * unknown and absent remain distinct even though both expand as unknown. */
static bool prop_dep_matches(const msb_prop_dep_t *dep, const msb_prop_t *p) {
    msb_work(SKIP_ONE);
    if (dep->present != (p != NULL)) {
        return false;
    }
    if (!p) {
        return true;
    }
    if ((dep->value != NULL) != (p->value != NULL)) {
        return false;
    }
    if (!p->value) {
        return true;
    }
    if (dep->bytes != p->capacity) {
        return false;
    }
    msb_work(dep->bytes);
    return memcmp(dep->value, p->value, dep->bytes) == 0;
}

/* Context-owned persistent state. Compressed radix branches split on property
 * or file IDs; identity belongs to immutable content, never to a pool address. */
enum { ST_PROPS, ST_SEEN, ST_POISON, ST_DOMAINS, ST_SLOTS = 64 };
typedef struct st_value {
    unsigned refs;
    msb_prop_t prop;
} st_value_t;
typedef struct st_slab st_slab_t;
struct st_node {
    st_node_t *child[2];
    st_node_t *next;
    st_slab_t *slab;
    st_value_t *value;
    uint64_t revision;
    uint64_t witness;
    uint64_t epoch;
    uint32_t key;
    unsigned refs;
    int bit;
    unsigned char domain;
    bool dependency;
    bool present;
    bool all_present;
    bool all_absent;
    bool witness_valid;
    bool witnessed;
};
struct st_slab {
    st_slab_t *next, *prev;
    st_slab_t *available_next, *available_prev;
    st_slab_t *empty_next, *empty_prev;
    bool on_empty;
    st_node_t *free;
    unsigned used;
    st_node_t slots[ST_SLOTS];
};
struct st_rope {
    st_rope_t *left, *right, *next;
    msb_item_t item;
    unsigned refs;
    int height;
    int count;
};
struct st_component {
    msb_view_t writes, inputs;
    st_rope_t *items;
    st_component_t *parent;
    int file;
    unsigned refs;
    bool poison, retain;
    bool open;
    int unevaluable, outside;
    bool start_open;
    int start_unevaluable, start_outside;
};
struct msb_state {
    CBMHashTable *symbols;
    CBMArena names;
    uint32_t next_symbol;
    st_slab_t *slabs, *available, *empty;
    st_component_t **components;
    int files;
    uint64_t generation, epoch, revision, audited_revision;
    size_t bytes;
    st_rope_t *item_a, *item_b;
    st_rope_t *capture_a, *capture_b;
};

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
#define ST_STAT(name) atomic_fetch_add_explicit(&msb_state_##name, 1, memory_order_relaxed)
#else
#define ST_STAT(name) ((void)0)
#endif

static st_node_t *st_hold(st_node_t *n) {
    if (n) {
        n->refs++;
        msb_work(SKIP_ONE);
    }
    return n;
}
static st_value_t *st_value_hold(st_value_t *v) {
    if (v) {
        v->refs++;
        msb_work(SKIP_ONE);
    }
    return v;
}
static void st_value_drop(msb_state_t *s, st_value_t *v) {
    if (v) {
        msb_work(SKIP_ONE);
        if (--v->refs == 0) {
            s->bytes -= sizeof(*v) + v->prop.capacity;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
            atomic_fetch_sub_explicit(&msb_value_live_counter, v->prop.capacity,
                                      memory_order_relaxed);
#endif
            cbm_free(CBM_MEM_CLASS_OTHER, v);
        }
    }
}
static void st_available_remove(msb_state_t *s, st_slab_t *b) {
    if (b->available_prev)
        b->available_prev->available_next = b->available_next;
    else
        s->available = b->available_next;
    if (b->available_next)
        b->available_next->available_prev = b->available_prev;
    b->available_next = b->available_prev = NULL;
}
static void st_available_add(msb_state_t *s, st_slab_t *b) {
    b->available_next = s->available;
    if (s->available)
        s->available->available_prev = b;
    s->available = b;
}
static void st_empty_remove(msb_state_t *s, st_slab_t *b) {
    if (!b->on_empty)
        return;
    if (b->empty_prev)
        b->empty_prev->empty_next = b->empty_next;
    else
        s->empty = b->empty_next;
    if (b->empty_next)
        b->empty_next->empty_prev = b->empty_prev;
    b->empty_next = b->empty_prev = NULL;
    b->on_empty = false;
}
static void st_empty_add(msb_state_t *s, st_slab_t *b) {
    b->empty_next = s->empty;
    b->empty_prev = NULL;
    if (s->empty)
        s->empty->empty_prev = b;
    s->empty = b;
    b->on_empty = true;
}
/* Intrusive zero-ref queue: neither radix nor component/rope depth consumes
 * the C call stack, and returning slots cannot allocate. Slabs stay alive. */
static void st_drop(msb_state_t *s, st_node_t *n) {
    st_node_t *pending = NULL;
    if (n && --n->refs == 0) {
        n->next = pending;
        pending = n;
    }
    while (pending) {
        n = pending;
        pending = n->next;
        msb_work(SKIP_ONE);
        for (int i = 0; i < PAIR_LEN; i++) {
            st_node_t *c = n->child[i];
            if (c && --c->refs == 0) {
                c->next = pending;
                pending = c;
            }
        }
        st_value_drop(s, n->value);
        st_slab_t *b = n->slab;
        if (!b->free)
            st_available_add(s, b);
        n->next = b->free;
        b->free = n;
        if (--b->used == 0)
            st_empty_add(s, b);
    }
}
static void st_view_drop(msb_state_t *s, msb_view_t *v) {
    for (int d = 0; d < ST_DOMAINS; d++) {
        st_drop(s, v->root[d]);
        v->root[d] = NULL;
    }
}
static st_node_t *st_new(msb_eval_t *ev, cbm_msb_node_fail_operation_t phase) {
    msb_state_t *s = ev->state;
    if (ev->oom || s->revision == UINT64_MAX) {
        ev->oom = true;
        return NULL;
    }
    /* This is a real slot acquisition. Empty/identity operations never call it. */
    if (msb_fail_node_alloc(phase) || msb_fail_item_operation(ev->item_alloc_operation)) {
        ev->oom = true;
        return NULL;
    }
    st_slab_t *b = s->available;
    if (!b) {
        b = (st_slab_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*b));
        if (!b) {
            ev->oom = true;
            return NULL;
        }
        s->bytes += sizeof(*b);
        b->next = s->slabs;
        if (s->slabs)
            s->slabs->prev = b;
        s->slabs = b;
        for (int i = ST_SLOTS; i > 0;) {
            st_node_t *n = &b->slots[--i];
            n->slab = b;
            n->next = b->free;
            b->free = n;
        }
        st_available_add(s, b);
        msb_work(sizeof(*b));
        ev_peak(ev);
    }
    st_empty_remove(s, b);
    st_node_t *n = b->free;
    uint64_t previous = n->revision;
    bool witnessed = n->witnessed;
    b->free = n->next;
    b->used++;
    if (!b->free)
        st_available_remove(s, b);
    memset(n, 0, sizeof(*n));
    n->slab = b;
    n->refs = 1;
    n->bit = -1;
    n->revision = ++s->revision;
    n->epoch = s->epoch;
    ST_STAT(allocations);
    if (previous) {
        ST_STAT(slot_reuses);
        if (witnessed)
            ST_STAT(witnessed_slot_reuses);
    }
    if (!n->revision || n->revision <= previous || n->revision <= s->audited_revision) {
        ST_STAT(revision_errors);
    }
    s->audited_revision = n->revision;
    msb_work(sizeof(*n));
    return n;
}
static int st_high_bit(uint32_t key) {
    int bit = -1;
    while (key) {
        key >>= 1;
        bit++;
        msb_work(SKIP_ONE);
    }
    return bit;
}
static bool st_same_range(uint32_t a, uint32_t b, int bit) {
    return bit == 31 || (a >> (bit + 1)) == (b >> (bit + 1));
}
static st_node_t *st_get(st_node_t *n, uint32_t key) {
    while (n && n->bit >= 0) {
        msb_work(SKIP_ONE);
        if (!st_same_range(n->key, key, n->bit))
            return NULL;
        n = n->child[(key >> n->bit) & 1];
    }
    msb_work(SKIP_ONE);
    return n && n->key == key ? n : NULL;
}
/* Restrict a map to the exact dependency prefix, not merely its depth. */
static st_node_t *st_region(st_node_t *n, uint32_t key, int bit) {
    while (n && n->bit > bit) {
        msb_work(SKIP_ONE);
        if (!st_same_range(n->key, key, n->bit))
            return NULL;
        n = n->child[(key >> n->bit) & 1];
    }
    msb_work(SKIP_ONE);
    return n && st_same_range(n->key, key, bit) ? n : NULL;
}
static bool st_value_equal(st_value_t *a, st_value_t *b) {
    msb_work(SKIP_ONE);
    if (a == b)
        return true;
    if (!a || !b || a->prop.capacity != b->prop.capacity)
        return false;
    msb_work(a->prop.capacity);
    return memcmp(a->prop.value, b->prop.value, a->prop.capacity) == 0;
}
static bool st_expected_equal(st_node_t *a, st_node_t *b) {
    return a->present == b->present && st_value_equal(a->value, b->value);
}
static void st_certify(msb_eval_t *ev, st_node_t *dep, st_node_t *view, int domain);
static st_node_t *st_branch(msb_eval_t *ev, st_node_t *l, st_node_t *r, int bit, bool dep,
                            int domain, cbm_msb_node_fail_operation_t phase) {
    if (!l)
        return st_hold(r);
    if (!r)
        return st_hold(l);
    if (!dep && ev->reuse_nodes) {
        /* One borrowed root exists only during this operation. A bounded
         * radix lookup may reuse an exact state branch; witnesses do not
         * establish equivalence, and no pair table is retained. */
        st_node_t *same = st_region(ev->reuse_nodes, l->key, bit);
        if (same && !same->dependency && same->domain == domain &&
            same->epoch == ev->state->epoch && same->bit == bit && same->key == l->key &&
            same->child[0] == l && same->child[1] == r)
            return st_hold(same);
    }
    st_node_t *n = st_new(ev, phase);
    if (n) {
        n->child[0] = st_hold(l);
        n->child[1] = st_hold(r);
        n->bit = bit;
        n->key = l->key;
        n->dependency = dep;
        n->domain = (unsigned char)domain;
        n->all_present = l->all_present && r->all_present;
        n->all_absent = dep && l->all_absent && r->all_absent;
        /* A fresh join starts without a witness. Certify every constraint
         * explicitly before recording an aligned witness for this branch. */
        if (dep)
            st_certify(ev, n, ev->view.root[domain], domain);
    }
    return n;
}
/* Immutable ordered overlay/union. Each recursive call eliminates a radix
 * level. Unchanged/empty subtrees are shared; there is no pair-operation cache. */
static st_node_t *st_union(msb_eval_t *ev, st_node_t *a, st_node_t *b, bool deps, int domain,
                           cbm_msb_node_fail_operation_t phase) {
    msb_work(SKIP_ONE);
    if (a == b || !b)
        return st_hold(a);
    if (!a)
        return st_hold(b);
    if (ev->oom)
        return NULL;
    int split = st_high_bit(a->key ^ b->key);
    int top = a->bit > b->bit ? a->bit : b->bit;
    if (split > top) {
        return ((a->key >> split) & 1) ? st_branch(ev, b, a, split, deps, domain, phase)
                                       : st_branch(ev, a, b, split, deps, domain, phase);
    }
    if (top < 0) {
        if (deps && !st_expected_equal(a, b)) {
            ev->oom = true;
            return NULL;
        }
        return st_hold(a);
    }
    st_node_t *ac[2] = {NULL, NULL}, *bc[2] = {NULL, NULL};
    if (a->bit == top) {
        ac[0] = a->child[0];
        ac[1] = a->child[1];
    } else
        ac[(a->key >> top) & 1] = a;
    if (b->bit == top) {
        bc[0] = b->child[0];
        bc[1] = b->child[1];
    } else
        bc[(b->key >> top) & 1] = b;
    st_node_t *l = st_union(ev, ac[0], bc[0], deps, domain, phase);
    st_node_t *r = st_union(ev, ac[1], bc[1], deps, domain, phase);
    st_node_t *out = NULL;
    if (!ev->oom) {
        if (a->bit == top && l == a->child[0] && r == a->child[1])
            out = st_hold(a);
        else if (b->bit == top && l == b->child[0] && r == b->child[1])
            out = st_hold(b);
        else
            out = st_branch(ev, l, r, top, deps, domain, phase);
    }
    st_drop(ev->state, l);
    st_drop(ev->state, r);
    if (out && out != a && a->bit == top && out->revision == a->revision)
        ST_STAT(revision_errors);
    if (out && out != b && b->bit == top && out->revision == b->revision)
        ST_STAT(revision_errors);
    return out;
}
static bool st_validate(msb_eval_t *ev, st_node_t *dep, st_node_t *view, int domain) {
    msb_work(SKIP_ONE);
    if (!dep)
        return true;
    st_node_t *at = st_region(view, dep->key, dep->bit);
    /* Exact absence constraints match an empty aligned range independently
     * of any historical revision witness. Present unknown is not absent. */
    if (!at && dep->dependency && dep->all_absent && dep->domain == domain &&
        dep->epoch == ev->state->epoch)
        return true;
    if (dep->witness_valid && dep->epoch == ev->state->epoch && dep->domain == domain &&
        dep->witness == (at ? at->revision : 0)) {
        ST_STAT(witness_skips);
        return true;
    }
    if (dep->bit < 0) {
        if (dep->present != (at != NULL))
            return false;
        return !at || st_value_equal(dep->value, at->value);
    }
    return st_validate(ev, dep->child[0], at, domain) && st_validate(ev, dep->child[1], at, domain);
}
/* Only unpublished nodes may gain a witness. All constraints, kind, epoch
 * and aligned range are checked; irrelevant historical state is not retained. */
static void st_certify(msb_eval_t *ev, st_node_t *dep, st_node_t *view, int domain) {
    if (!dep || dep->refs != 1 || dep->witness_valid)
        return;
    if (st_validate(ev, dep, view, domain)) {
        st_node_t *at = st_region(view, dep->key, dep->bit);
        dep->witness = at ? at->revision : 0;
        dep->witness_valid = true;
        dep->epoch = ev->state->epoch;
        dep->domain = (unsigned char)domain;
        if (at)
            at->witnessed = true;
    }
}
static st_node_t *st_mask(msb_eval_t *ev, st_node_t *dep, st_node_t *writes, int domain) {
    msb_work(SKIP_ONE);
    if (!dep || !writes)
        return st_hold(dep);
    if (dep == writes)
        return NULL;
    st_node_t *at = st_region(writes, dep->key, dep->bit);
    if (!at)
        return st_hold(dep);
    /* An aligned witness plus all-present constraints proves that every
     * dependency key is written here, without walking a shared key domain. */
    if (dep->all_present && dep->witness_valid && dep->epoch == ev->state->epoch &&
        dep->domain == domain && dep->witness == at->revision)
        return NULL;
    if (dep->bit < 0)
        return NULL;
    st_node_t *l = st_mask(ev, dep->child[0], at, domain);
    st_node_t *r = st_mask(ev, dep->child[1], at, domain);
    st_node_t *out = NULL;
    if (!ev->oom) {
        if (l == dep->child[0] && r == dep->child[1])
            out = st_hold(dep);
        else {
            out = st_branch(ev, l, r, dep->bit, true, domain, CBM_MSB_NODE_FAIL_DEPENDENCY);
            if (out && out->bit == dep->bit) {
                out->witness_valid = dep->witness_valid;
                out->witness = dep->witness;
                out->epoch = dep->epoch;
            }
        }
    }
    st_drop(ev->state, l);
    st_drop(ev->state, r);
    return out;
}
static uint32_t st_symbol(msb_eval_t *ev, const char *key) {
    msb_state_t *s = ev->state;
    msb_work(SKIP_ONE);
    uintptr_t id = (uintptr_t)cbm_ht_get(s->symbols, key);
    if (id)
        return (uint32_t)id;
    if (s->next_symbol == UINT32_MAX) {
        ev->oom = true;
        return 0;
    }
    size_t n = strlen(key);
    char *owned = cbm_arena_strndup(&s->names, key, n);
    if (!owned) {
        ev->oom = true;
        return 0;
    }
    id = ++s->next_symbol;
    cbm_ht_set(s->symbols, owned, (void *)id);
    msb_work(n + 3);
    if ((uintptr_t)cbm_ht_get(s->symbols, owned) != id) {
        ev->oom = true;
        return 0;
    }
    ev_peak(ev);
    return (uint32_t)id;
}
static void st_record_input(msb_eval_t *ev, msb_view_t *inputs, int domain, uint32_t key) {
    if (ev->oom || st_get(inputs->root[domain], key))
        return;
    st_node_t *current = st_get(ev->view.root[domain], key);
    st_node_t *dep = st_new(ev, CBM_MSB_NODE_FAIL_DEPENDENCY);
    if (!dep)
        return;
    dep->dependency = true;
    dep->domain = (unsigned char)domain;
    dep->key = key;
    dep->present = current != NULL;
    dep->all_present = dep->present;
    dep->all_absent = !dep->present;
    dep->value = current ? st_value_hold(current->value) : NULL;
    dep->witness_valid = true;
    dep->witness = current ? current->revision : 0;
    if (current)
        current->witnessed = true;
    st_node_t *next =
        st_union(ev, inputs->root[domain], dep, true, domain, CBM_MSB_NODE_FAIL_DEPENDENCY);
    st_drop(ev->state, dep);
    if (!ev->oom) {
        st_certify(ev, next, ev->view.root[domain], domain);
        st_drop(ev->state, inputs->root[domain]);
        inputs->root[domain] = next;
    } else
        st_drop(ev->state, next);
}
static const msb_prop_t *st_property(msb_eval_t *ev, const char *key) {
    uint32_t id = st_symbol(ev, key);
    if (!id)
        return NULL;
    st_node_t *n = st_get(ev->view.root[ST_PROPS], id);
    if (ev->capture_inputs)
        st_record_input(ev, &ev->state_inputs, ST_PROPS, id);
    else if (ev->builder && !st_get(ev->builder->writes.root[ST_PROPS], id))
        st_record_input(ev, &ev->builder->inputs, ST_PROPS, id);
    static const msb_prop_t unknown = {0};
    return n ? (n->value ? &n->value->prop : &unknown) : NULL;
}
static bool st_membership(msb_eval_t *ev, uint32_t file, int domain) {
    bool has = st_get(ev->view.root[domain], file + 1) != NULL;
    if (ev->builder && !st_get(ev->builder->writes.root[domain], file + 1))
        st_record_input(ev, &ev->builder->inputs, domain, file + 1);
    return has;
}
static st_value_t *st_make_value(msb_eval_t *ev, const char *text) {
    if (!text)
        return NULL;
    size_t bytes = strlen(text) + 1;
    st_value_t *v = NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (!msb_fail_value_alloc())
#endif
    {
        v = (st_value_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*v) + bytes);
    }
    if (!v) {
        ev->oom = true;
        return NULL;
    }
    v->refs = 1;
    v->prop.value = (char *)(v + 1);
    v->prop.capacity = bytes;
    memcpy(v->prop.value, text, bytes);
    ev->state->bytes += sizeof(*v) + bytes;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    atomic_fetch_add_explicit(&msb_value_live_counter, bytes, memory_order_relaxed);
#endif
    msb_work(bytes + sizeof(*v));
    ev_peak(ev);
    return v;
}
static void st_write(msb_eval_t *ev, uint32_t key, int domain, const char *text) {
    if (ev->oom)
        return;
    st_node_t *old = st_get(ev->view.root[domain], key);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    /* Independent before/after audit of every existing ancestor on this
     * changed key's path. Snapshot IDs, not addresses that may be recycled. */
    struct {
        uint32_t key;
        int bit;
        uint64_t revision;
    } before[33];
    int before_count = 0;
    for (st_node_t *n = ev->view.root[domain]; n && before_count < 33;) {
        before[before_count].key = n->key;
        before[before_count].bit = n->bit;
        before[before_count++].revision = n->revision;
        if (n->bit < 0 || !st_same_range(n->key, key, n->bit))
            break;
        n = n->child[(key >> n->bit) & 1];
    }
#endif
    if (domain == ST_PROPS && old && old->value && text &&
        old->value->prop.capacity == strlen(text) + 1 && strcmp(old->value->prop.value, text))
        ST_STAT(same_length_value_changes);
    cbm_msb_node_fail_operation_t phase =
        ev->builder && ev->builder->retain ? CBM_MSB_NODE_FAIL_CAPTURE : CBM_MSB_NODE_FAIL_NONE;
    st_node_t *leaf = NULL;
    if (old &&
        ((!text && !old->value) || (text && old->value && !strcmp(text, old->value->prop.value))))
        leaf = st_hold(old);
    else {
        leaf = st_new(ev, phase);
        if (!leaf)
            return;
        leaf->key = key;
        leaf->domain = (unsigned char)domain;
        leaf->present = true;
        leaf->value = domain == ST_PROPS ? st_make_value(ev, text) : NULL;
    }
    st_node_t *effect = NULL;
    st_node_t *saved_reuse = ev->reuse_nodes;
    if (!ev->oom && ev->builder) {
        ev->reuse_nodes = ev->view.root[domain];
        effect = st_union(ev, leaf, ev->builder->writes.root[domain], false, domain, phase);
        ev->reuse_nodes = saved_reuse;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        bool is_new = !st_get(ev->builder->writes.root[domain], key);
        if (domain == ST_PROPS && is_new && msb_fail_prop_insert()) {
            st_drop(ev->state, effect);
            effect = st_hold(ev->builder->writes.root[domain]);
        }
#endif
        if (!st_get(effect, key))
            ev->oom = true;
    }
    ev->reuse_nodes = effect;
    st_node_t *view =
        !ev->oom ? st_union(ev, leaf, ev->view.root[domain], false, domain, phase) : NULL;
    ev->reuse_nodes = saved_reuse;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (!ev->builder && domain == ST_PROPS && !old && msb_fail_prop_insert()) {
        st_drop(ev->state, view);
        view = st_hold(ev->view.root[domain]);
    }
#endif
    if (!st_get(view, key))
        ev->oom = true;
    if (!ev->oom) {
        if (old && leaf != old && leaf->revision == old->revision)
            ST_STAT(revision_errors);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        if (leaf != old) {
            for (int i = 0; i < before_count; i++) {
                /* Only old ranges containing the changed key are affected. */
                if (!st_same_range(before[i].key, key, before[i].bit))
                    continue;
                st_node_t *after = view;
                while (after && after->bit > before[i].bit) {
                    if (!st_same_range(after->key, before[i].key, after->bit)) {
                        after = NULL;
                        break;
                    }
                    after = after->child[(before[i].key >> after->bit) & 1];
                }
                if (after && !st_same_range(after->key, before[i].key, before[i].bit))
                    after = NULL;
                if (after && after->revision == before[i].revision)
                    ST_STAT(revision_errors);
            }
        }
#endif
        st_drop(ev->state, ev->view.root[domain]);
        ev->view.root[domain] = view;
        if (ev->builder) {
            st_drop(ev->state, ev->builder->writes.root[domain]);
            ev->builder->writes.root[domain] = effect;
        }
    } else {
        st_drop(ev->state, view);
        st_drop(ev->state, effect);
    }
    st_drop(ev->state, leaf);
}
static void st_set_property(msb_eval_t *ev, const char *name, const char *value) {
    char key[MSB_NAME_MAX];
    if (!msb_key(name, strlen(name), key))
        return;
    uint32_t id = st_symbol(ev, key);
    if (id)
        st_write(ev, id, ST_PROPS, value);
}

static int seed_slot(const char *key) {
    for (int i = 0; i < MSB_SEEDS; i++) {
        msb_work(SKIP_ONE);
        if (strcmp(key, MSB_SEED_NAMES[i]) == 0) {
            return i;
        }
    }
    return CBM_NOT_FOUND;
}

static msb_prop_dep_t *copy_prop_dep(msb_eval_t *ev, const msb_prop_t *p) {
    msb_prop_dep_t *dep = (msb_prop_dep_t *)ev_alloc(ev, sizeof(*dep));
    if (!dep) {
        return NULL;
    }
    *dep = (msb_prop_dep_t){.present = p != NULL};
    msb_work(sizeof(*dep));
    if (p && p->value) {
        dep->bytes = p->capacity;
        dep->value = ev_strndup(ev, p->value, dep->bytes - SKIP_ONE);
    }
    return ev->oom ? NULL : dep;
}

/* Record reads that escape the capturing owner's local state. Incoming
 * state is immutable for this pass. Target effects use the prefix baseline;
 * final items also use completed target writes. Dependency values are owned
 * copies, so no project buffer survives through a dependency pointer. */
static void record_prop_input(msb_eval_t *ev, const char *key, const msb_prop_t *p) {
    int seed = seed_slot(key);
    if (seed >= 0) {
        if (!ev->inputs.seeds[seed]) {
            ev->inputs.seeds[seed] = copy_prop_dep(ev, p);
        }
        return;
    }
    msb_work(SKIP_ONE);
    if (cbm_ht_get(ev->inputs.props, key)) {
        return;
    }
    msb_prop_dep_t *dep = copy_prop_dep(ev, p);
    char *owned_key = dep ? ev_strndup(ev, key, strlen(key)) : NULL;
    if (!owned_key) {
        return;
    }
    const msb_prop_t *baseline = NULL;
    if (ev->incoming->effects) {
        msb_work(SKIP_ONE);
        baseline = (const msb_prop_t *)cbm_ht_get(ev->incoming->effects->props, key);
    }
    if (!baseline && ev->incoming->base) {
        msb_work(SKIP_ONE);
        baseline = (const msb_prop_t *)cbm_ht_get(ev->incoming->base->props, key);
    }
    dep->exception = !prop_dep_matches(dep, baseline);
    msb_work(PAIR_LEN);
    cbm_ht_set(ev->inputs.props, owned_key, dep);
    if (cbm_ht_get(ev->inputs.props, owned_key) != dep) {
        ev->oom = true;
        return;
    }
    ev->inputs.prop_exceptions += dep->exception;
}

/* A present unknown value shadows lower layers just like a known one.
 * During target capture, incoming is a separate read-only owner; during
 * pass 2, completed target writes are the highest-precedence layer. */
static const msb_prop_t *prop_lookup(msb_eval_t *ev, const char *key) {
    if (ev->state)
        return st_property(ev, key);
    const msb_prop_t *p = NULL;
    if (ev->effects) {
        msb_work(SKIP_ONE);
        p = (const msb_prop_t *)cbm_ht_get(ev->effects->props, key);
        if (p) {
            return p;
        }
    }
    if (ev->props) {
        msb_work(SKIP_ONE);
        p = (const msb_prop_t *)cbm_ht_get(ev->props, key);
    }
    if (!p && ev->incoming) {
        p = prop_lookup(ev->incoming, key);
        if (ev->capture_inputs) {
            record_prop_input(ev, key, p);
        }
        return p;
    }
    if (!p && ev->base) {
        msb_work(SKIP_ONE);
        p = (const msb_prop_t *)cbm_ht_get(ev->base->props, key);
    }
    if (!p && ev->seeds) {
        msb_work(SKIP_ONE);
        p = (const msb_prop_t *)cbm_ht_get(ev->seeds->props, key);
        if (p && ev->track_seed_reads) {
            ev->seed_read = true;
        }
    }
    return p;
}

/* The value of $(name) read in `file`; NULL when it is not known. */
static const char *msb_ref(msb_eval_t *ev, int file, const char *name, size_t len) {
    char key[MSB_NAME_MAX];
    if (!msb_key(name, len, key)) {
        return NULL;
    }
    static const char this_file[] = "msbuildthisfile";
    if (strncmp(key, this_file, sizeof(this_file) - SKIP_ONE) == 0) {
        const msb_file_t *f = &ev->m->files[file];
        const char *rest = key + sizeof(this_file) - SKIP_ONE;
        if (strcmp(rest, "directory") == 0) {
            return f->abs_dir;
        }
        if (rest[0] == '\0') {
            return f->name;
        }
        if (strcmp(rest, "name") == 0) {
            return f->stem;
        }
        if (strcmp(rest, "extension") == 0) {
            return f->ext;
        }
        if (strcmp(rest, "fullpath") == 0) {
            return f->abs_path;
        }
    }
    const msb_prop_t *p = prop_lookup(ev, key);
    return p ? p->value : NULL;
}

/* A value being put together; `over` once it is longer than a value may be. */
typedef struct {
    msb_eval_t *ev;
    char *buf;
    size_t len;
    size_t cap;
    bool over;
} msb_sb_t;

static void msb_sb_put(msb_sb_t *b, const char *s, size_t n) {
    if (b->over || b->ev->oom) {
        return;
    }
    if (b->len + n >= MSB_VALUE_MAX) {
        b->over = true;
        return;
    }
    if (b->len + n + SKIP_ONE > b->cap) {
        size_t ncap = b->cap ? b->cap * PAIR_LEN : CBM_SZ_64;
        while (ncap < b->len + n + SKIP_ONE) {
            ncap *= PAIR_LEN;
        }
        char *grown = (char *)scratch_alloc(b->ev, ncap);
        if (!grown) {
            return;
        }
        if (b->len > 0) {
            memcpy(grown, b->buf, b->len);
            msb_work(b->len);
        }
        b->buf = grown;
        b->cap = ncap;
    }
    memcpy(b->buf + b->len, s, n);
    b->len += n;
    b->buf[b->len] = '\0';
    msb_work(n + SKIP_ONE); /* append plus its terminator */
}

/* `text` with its $(Property) references replaced. NULL when it has no value
 * this reader knows: a property that is not known, a property function, an
 * item or metadata reference, an escape, a value past MSB_VALUE_MAX. */
static const char *msb_expand(msb_eval_t *ev, int file, const char *text) {
    msb_sb_t b = {.ev = ev};
    for (const char *p = text; *p;) {
        if (p[0] == '$' && p[1] == '(') {
            const char *nm = p + PAIR_LEN;
            const char *e = nm;
            while (isalnum((unsigned char)*e) || *e == '_') {
                e++;
            }
            if (e == nm || *e != ')' || isdigit((unsigned char)nm[0])) {
                return NULL;
            }
            const char *v = msb_ref(ev, file, nm, (size_t)(e - nm));
            if (!v) {
                return NULL;
            }
            msb_sb_put(&b, v, strlen(v));
            p = e + SKIP_ONE;
            continue;
        }
        if ((p[0] == '@' || p[0] == '%') && p[1] == '(') {
            return NULL;
        }
        if (p[0] == '%' && isxdigit((unsigned char)p[1]) && isxdigit((unsigned char)p[2])) {
            return NULL;
        }
        msb_sb_put(&b, p, SKIP_ONE);
        p++;
    }
    if (b.over || ev->oom) {
        return NULL;
    }
    return b.buf ? b.buf : "";
}

/* ── Conditions ──────────────────────────────────────────────────── */

static msb_tri_t tri_not(msb_tri_t v) {
    return v == MSB_UNKNOWN ? MSB_UNKNOWN : (v == MSB_TRUE ? MSB_FALSE : MSB_TRUE);
}

static msb_tri_t tri_and(msb_tri_t a, msb_tri_t b) {
    if (a == MSB_FALSE || b == MSB_FALSE) {
        return MSB_FALSE;
    }
    return (a == MSB_TRUE && b == MSB_TRUE) ? MSB_TRUE : MSB_UNKNOWN;
}

static msb_tri_t tri_or(msb_tri_t a, msb_tri_t b) {
    if (a == MSB_TRUE || b == MSB_TRUE) {
        return MSB_TRUE;
    }
    return (a == MSB_FALSE && b == MSB_FALSE) ? MSB_FALSE : MSB_UNKNOWN;
}

static bool ci_eq_n(const char *a, size_t an, const char *b, size_t bn) {
    if (an != bn) {
        return false;
    }
    for (size_t i = 0; i < an; i++) {
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i])) {
            return false;
        }
    }
    return true;
}

static bool ci_is(const char *a, size_t an, const char *word) {
    return ci_eq_n(a, an, word, strlen(word));
}

/* A string MSBuild converts to a boolean: 1, 0, or -1 for any other. */
static int msb_bool(const char *s, size_t n) {
    if (ci_is(s, n, "true") || ci_is(s, n, "on") || ci_is(s, n, "yes")) {
        return SKIP_ONE;
    }
    if (ci_is(s, n, "false") || ci_is(s, n, "off") || ci_is(s, n, "no")) {
        return 0;
    }
    return CBM_NOT_FOUND;
}

/* A decimal number as its parts, leading and trailing zeros dropped. */
typedef struct {
    bool neg;
    const char *ip;
    size_t il;
    const char *fp;
    size_t fl;
} msb_num_t;

/* [+-]digits[.digits] or [+-].digits, nothing else. */
static bool msb_decimal(const char *s, size_t n, msb_num_t *out) {
    size_t i = 0;
    memset(out, 0, sizeof(*out));
    if (i < n && (s[i] == '+' || s[i] == '-')) {
        out->neg = s[i] == '-';
        i++;
    }
    size_t is = i;
    while (i < n && isdigit((unsigned char)s[i])) {
        i++;
    }
    size_t ie = i;
    size_t fs = i;
    size_t fe = i;
    if (i < n && s[i] == '.') {
        i++;
        fs = i;
        while (i < n && isdigit((unsigned char)s[i])) {
            i++;
        }
        fe = i;
    }
    if (i != n || (ie == is && fe == fs)) {
        return false;
    }
    bool nonzero = false;
    for (size_t k = is; k < fe; k++) {
        nonzero = nonzero || (s[k] != '0' && s[k] != '.');
    }
    while (is < ie && s[is] == '0') {
        is++;
    }
    while (fe > fs && s[fe - SKIP_ONE] == '0') {
        fe--;
    }
    out->ip = s + is;
    out->il = ie - is;
    out->fp = s + fs;
    out->fl = fe - fs;
    if (!nonzero) {
        out->neg = false; /* -0 is 0 */
    }
    return true;
}

/* Text some conversion could read as a number although msb_decimal does not
 * (hexadecimal, an exponent). */
static bool msb_numberish(const char *s, size_t n) {
    if (n == 0 || !(isdigit((unsigned char)s[0]) || s[0] == '+' || s[0] == '-' || s[0] == '.')) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!(isxdigit(c) || c == 'x' || c == 'X' || c == '.' || c == '+' || c == '-')) {
            return false;
        }
    }
    return true;
}

/* MSBuild's `==` on two expanded operands, as far as it can be known. */
static msb_tri_t msb_equal_spans(const char *a, size_t an, const char *b, size_t bn) {
    if (ci_eq_n(a, an, b, bn)) {
        return MSB_TRUE; /* the same text is equal under every conversion */
    }
    /* An absolute path: where the repository lies is not known here, so its
     * real text is not. Two of them differ as their marked texts do; one is
     * never the empty string; any other comparison is open. */
    bool ma = an > 0 && a[0] == MSB_PATH_MARK;
    bool mb = bn > 0 && b[0] == MSB_PATH_MARK;
    if (memchr(a + ma, MSB_PATH_MARK, an - ma) || memchr(b + mb, MSB_PATH_MARK, bn - mb)) {
        return MSB_UNKNOWN;
    }
    if (ma || mb) {
        return ((ma && mb) || an == 0 || bn == 0) ? MSB_FALSE : MSB_UNKNOWN;
    }
    int ba = msb_bool(a, an);
    int bb = msb_bool(b, bn);
    if (ba >= 0 && bb >= 0) {
        return ba == bb ? MSB_TRUE : MSB_FALSE;
    }
    msb_num_t na;
    msb_num_t nb;
    bool da = msb_decimal(a, an, &na);
    bool db = msb_decimal(b, bn, &nb);
    if (da && db) {
        if (na.il + na.fl > MSB_SIGNIFICANT || nb.il + nb.fl > MSB_SIGNIFICANT) {
            return MSB_UNKNOWN; /* MSBuild compares doubles: these may round together */
        }
        bool same = na.neg == nb.neg && na.il == nb.il && na.fl == nb.fl &&
                    memcmp(na.ip, nb.ip, na.il) == 0 && memcmp(na.fp, nb.fp, na.fl) == 0;
        return same ? MSB_TRUE : MSB_FALSE;
    }
    if ((da || msb_numberish(a, an)) && (db || msb_numberish(b, bn))) {
        return MSB_UNKNOWN;
    }
    return MSB_FALSE;
}

static void trim_span(const char **s, size_t *n) {
    while (*n > 0 && isspace((unsigned char)(*s)[0])) {
        (*s)++;
        (*n)--;
    }
    while (*n > 0 && isspace((unsigned char)(*s)[*n - SKIP_ONE])) {
        (*n)--;
    }
}

/* A property's value keeps the white space its element was written with.
 * Whether a comparison sees it is not something to guess: the two readings
 * must agree. */
static msb_tri_t msb_equal(const char *a, const char *b) {
    size_t an = strlen(a);
    size_t bn = strlen(b);
    msb_tri_t as_written = msb_equal_spans(a, an, b, bn);
    trim_span(&a, &an);
    trim_span(&b, &bn);
    msb_tri_t trimmed = msb_equal_spans(a, an, b, bn);
    return as_written == trimmed ? as_written : MSB_UNKNOWN;
}

/* A value standing alone as a condition. */
static msb_tri_t msb_truth(const char *v) {
    size_t n = strlen(v);
    int as_written = msb_bool(v, n);
    trim_span(&v, &n);
    int trimmed = msb_bool(v, n);
    if (as_written != trimmed || trimmed < 0) {
        return MSB_UNKNOWN;
    }
    return trimmed ? MSB_TRUE : MSB_FALSE;
}

typedef struct {
    msb_eval_t *ev;
    int file;
    const char *s;
    int depth;
    bool bad; /* not a condition this reader can parse */
} msb_cond_t;

static void cond_ws(msb_cond_t *c) {
    while (isspace((unsigned char)*c->s)) {
        c->s++;
    }
}

/* The keyword `kw` (lower case) stands next: consume it. */
static bool cond_keyword(msb_cond_t *c, const char *kw) {
    cond_ws(c);
    size_t n = strlen(kw);
    for (size_t i = 0; i < n; i++) {
        if (tolower((unsigned char)c->s[i]) != kw[i]) {
            return false;
        }
    }
    unsigned char after = (unsigned char)c->s[n];
    if (isalnum(after) || after == '_') {
        return false;
    }
    c->s += n;
    return true;
}

/* Past the group opened by the '(' at c->s (quotes respected); false when it
 * does not close. */
static bool cond_skip_group(msb_cond_t *c) {
    int depth = 0;
    for (const char *p = c->s; *p; p++) {
        if (*p == '\'') {
            p = strchr(p + SKIP_ONE, '\'');
            if (!p) {
                return false;
            }
        } else if (*p == '(') {
            depth++;
        } else if (*p == ')' && --depth == 0) {
            c->s = p + SKIP_ONE;
            return true;
        }
    }
    return false;
}

/* An operand: 'text', $(Property), a bare word, or a function call. Returns
 * its value, NULL when that is not known; *ok false when no operand stands
 * here. */
static const char *cond_value(msb_cond_t *c, bool *ok) {
    cond_ws(c);
    *ok = true;
    const char *s = c->s;
    if (*s == '\'') {
        const char *e = strchr(s + SKIP_ONE, '\'');
        if (!e) {
            *ok = false;
            return NULL;
        }
        c->s = e + SKIP_ONE;
        const char *lit = scratch_strndup(c->ev, s + SKIP_ONE, (size_t)(e - s - SKIP_ONE));
        return lit ? msb_expand(c->ev, c->file, lit) : NULL;
    }
    if (s[0] == '$' && s[1] == '(') {
        c->s = s + SKIP_ONE;
        if (!cond_skip_group(c)) {
            *ok = false;
            return NULL;
        }
        const char *ref = scratch_strndup(c->ev, s, (size_t)(c->s - s));
        return ref ? msb_expand(c->ev, c->file, ref) : NULL;
    }
    const char *e = s;
    while (isalnum((unsigned char)*e) || *e == '_' || *e == '.' || *e == '-' || *e == '+') {
        e++;
    }
    if (e == s) {
        *ok = false;
        return NULL;
    }
    c->s = e;
    cond_ws(c);
    if (*c->s == '(') {
        *ok = cond_skip_group(c); /* Exists(...), HasTrailingSlash(...): not evaluated */
        return NULL;
    }
    return scratch_strndup(c->ev, s, (size_t)(e - s));
}

static msb_tri_t cond_or(msb_cond_t *c);

static msb_tri_t cond_comparison_value(msb_cond_t *c) {
    bool ok = false;
    const char *lhs = cond_value(c, &ok);
    if (!ok) {
        c->bad = true;
        return MSB_UNKNOWN;
    }
    cond_ws(c);
    char op0 = c->s[0];
    char op1 = op0 ? c->s[1] : '\0';
    if ((op0 == '=' || op0 == '!') && op1 == '=') {
        c->s += PAIR_LEN;
        const char *rhs = cond_value(c, &ok);
        if (!ok) {
            c->bad = true;
            return MSB_UNKNOWN;
        }
        msb_tri_t v = (lhs && rhs) ? msb_equal(lhs, rhs) : MSB_UNKNOWN;
        return op0 == '!' ? tri_not(v) : v;
    }
    if (op0 == '<' || op0 == '>') {
        c->s += op1 == '=' ? PAIR_LEN : SKIP_ONE; /* an order of numbers or versions */
        (void)cond_value(c, &ok);
        c->bad = c->bad || !ok;
        return MSB_UNKNOWN;
    }
    return lhs ? msb_truth(lhs) : MSB_UNKNOWN;
}

/* Both operands must survive through comparison, including every error
 * return. Only the primitive result escapes to the surrounding condition. */
static msb_tri_t cond_comparison(msb_cond_t *c) {
    msb_tri_t result = cond_comparison_value(c);
    cbm_arena_reset(&c->ev->scratch);
    return result;
}

static msb_tri_t cond_primary(msb_cond_t *c) {
    cond_ws(c);
    bool negate = false;
    while (c->s[0] == '!' && c->s[1] != '=') {
        negate = !negate;
        c->s++;
        cond_ws(c);
    }
    msb_tri_t v = MSB_UNKNOWN;
    if (*c->s == '(') {
        if (c->depth >= MSB_COND_DEPTH) {
            c->bad = true;
            return MSB_UNKNOWN;
        }
        c->s++;
        c->depth++;
        v = cond_or(c);
        c->depth--;
        cond_ws(c);
        if (*c->s != ')') {
            c->bad = true;
            return MSB_UNKNOWN;
        }
        c->s++;
    } else {
        v = cond_comparison(c);
    }
    return negate ? tri_not(v) : v;
}

static msb_tri_t cond_and(msb_cond_t *c) {
    msb_tri_t v = cond_primary(c);
    while (!c->bad && cond_keyword(c, "and")) {
        v = tri_and(v, cond_primary(c));
    }
    return v;
}

static msb_tri_t cond_or(msb_cond_t *c) {
    msb_tri_t v = cond_and(c);
    while (!c->bad && cond_keyword(c, "or")) {
        v = tri_or(v, cond_and(c));
    }
    return v;
}

/* A Condition attribute, read in `file`. `and` binds tighter than `or`. */
static msb_tri_t msb_cond(msb_eval_t *ev, int file, const char *cond) {
    if (!cond) {
        return MSB_TRUE;
    }
    msb_cond_t c = {.ev = ev, .file = file, .s = cond};
    cond_ws(&c);
    if (!*c.s) {
        return MSB_TRUE;
    }
    msb_tri_t v = cond_or(&c);
    cond_ws(&c);
    return (c.bad || *c.s) ? MSB_UNKNOWN : v;
}

/* ── Imports ─────────────────────────────────────────────────────── */

/* Resolve "." and ".." in a '/'-separated path, in place. false when it
 * leaves the repository. */
static bool msb_normalize(char *path) {
    /* The text is read by index up to its original length: what is written
     * (every kept segment and a '/' after it) never passes the read position,
     * but it does overwrite the terminator. */
    size_t len = strlen(path);
    size_t w = 0;
    size_t i = 0;
    while (i < len) {
        size_t s = i;
        while (i < len && path[i] != '/') {
            i++;
        }
        size_t n = i - s;
        i += i < len; /* past the '/' */
        if (n == PAIR_LEN && path[s] == '.' && path[s + SKIP_ONE] == '.') {
            if (w == 0) {
                return false;
            }
            w--; /* the '/' that ends the previous segment */
            while (w > 0 && path[w - SKIP_ONE] != '/') {
                w--;
            }
        } else if (n > 0 && !(n == SKIP_ONE && path[s] == '.')) {
            memmove(path + w, path + s, n);
            msb_work(n + SKIP_ONE); /* copied segment and slash */
            w += n;
            path[w++] = '/';
        }
    }
    path[w > 0 ? w - SKIP_ONE : 0] = '\0';
    return true;
}

/* The file an <Import Project="..."> written in `file` names: its index, or
 * CBM_NOT_FOUND. What cannot be followed is counted: a path that is not
 * known or names several files (unevaluable), a file the index does not hold
 * (outside). */
static int msb_import_target(msb_eval_t *ev, int file, const char *project) {
    const char *value = project ? msb_expand(ev, file, project) : NULL;
    if (!value) {
        ev->unevaluable++;
        return CBM_NOT_FOUND;
    }
    size_t n = strlen(value);
    trim_span(&value, &n);
    const msb_file_t *f = &ev->m->files[file];
    bool marked = n > 0 && value[0] == MSB_PATH_MARK;
    size_t skip = marked ? SKIP_ONE : 0;
    if (n == skip || memchr(value + skip, MSB_PATH_MARK, n - skip) || memchr(value, '*', n) ||
        memchr(value, '?', n)) {
        ev->unevaluable++;
        return CBM_NOT_FOUND;
    }
    bool absolute =
        !marked && (value[0] == '/' || value[0] == '\\' || (n > SKIP_ONE && value[1] == ':'));
    if (absolute) {
        ev->outside++;
        return CBM_NOT_FOUND;
    }
    size_t dl = marked ? 0 : strlen(f->dir);
    char *path = (char *)scratch_alloc(ev, dl + n + PAIR_LEN);
    if (!path) {
        return CBM_NOT_FOUND;
    }
    memcpy(path, f->dir, dl);
    path[dl] = '/';
    memcpy(path + dl + SKIP_ONE, value + skip, n - skip);
    path[dl + SKIP_ONE + n - skip] = '\0';
    msb_work(dl + n - skip + PAIR_LEN);
    for (char *p = path; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
    int target = msb_normalize(path) ? msb_file_index(ev->m, path) : CBM_NOT_FOUND;
    ev->outside += target < 0;
    return target;
}

static bool set_has(const CBMHashTable *set, const char *key) {
    msb_work(SKIP_ONE);
    return cbm_ht_get(set, key) != NULL;
}

/* Stable borrowed sentinel: a frozen set must not retain a stack address. */
static const char MSB_SET_PRESENT = 0;

static void record_set_input(msb_eval_t *ev, const char *key, bool present, bool poison) {
    CBMHashTable *deps = poison ? ev->inputs.poisoned : ev->inputs.seen;
    msb_work(SKIP_ONE);
    if (cbm_ht_get(deps, key)) {
        return;
    }
    msb_set_dep_t *dep = (msb_set_dep_t *)ev_alloc(ev, sizeof(*dep));
    char *owned_key = dep ? ev_strndup(ev, key, strlen(key)) : NULL;
    if (!owned_key) {
        return;
    }
    const msb_eval_t *base = ev->incoming->base;
    bool baseline = base && set_has(poison ? base->poisoned : base->seen, key);
    *dep = (msb_set_dep_t){.present = present, .exception = present != baseline};
    msb_work(sizeof(*dep) + PAIR_LEN);
    cbm_ht_set(deps, owned_key, dep);
    if (cbm_ht_get(deps, owned_key) != dep) {
        ev->oom = true;
        return;
    }
    if (poison) {
        ev->inputs.poisoned_exceptions += dep->exception;
    } else {
        ev->inputs.seen_exceptions += dep->exception;
    }
}

static bool seen_has(msb_eval_t *ev, const char *key) {
    if (ev->state) {
        int file = msb_file_index(ev->m, key);
        return file >= 0 && st_membership(ev, (uint32_t)file, ST_SEEN);
    }
    if (set_has(ev->seen, key)) {
        return true;
    }
    bool present =
        ev->incoming ? seen_has(ev->incoming, key) : ev->base && set_has(ev->base->seen, key);
    if (ev->capture_inputs) {
        record_set_input(ev, key, present, false);
    }
    return present;
}

static bool poisoned_has(msb_eval_t *ev, const char *key) {
    if (ev->state) {
        int file = msb_file_index(ev->m, key);
        return file >= 0 && st_membership(ev, (uint32_t)file, ST_POISON);
    }
    if (set_has(ev->poisoned, key)) {
        return true;
    }
    bool present = ev->incoming ? poisoned_has(ev->incoming, key)
                                : ev->base && set_has(ev->base->poisoned, key);
    if (ev->capture_inputs) {
        record_set_input(ev, key, present, true);
    }
    return present;
}

/* The nearest file called `name` in `dir` or above it; CBM_NOT_FOUND for none. */
static int msb_nearest(msb_eval_t *ev, const char *dir, const char *name) {
    size_t dl = strlen(dir);
    size_t nl = strlen(name);
    char *path = (char *)scratch_alloc(ev, dl + nl + PAIR_LEN);
    if (!path) {
        return CBM_NOT_FOUND;
    }
    for (;;) {
        memcpy(path, dir, dl);
        path[dl] = '/';
        memcpy(path + dl + (dl ? SKIP_ONE : 0), name, nl + SKIP_ONE);
        msb_work(dl + nl + PAIR_LEN);
        msb_nearest_step();
        int found = msb_file_index(ev->m, path);
        if (found >= 0 || dl == 0) {
            return found;
        }
        while (dl > 0 && dir[dl - SKIP_ONE] != '/') {
            msb_work(SKIP_ONE);
            dl--;
        }
        dl = dl > 0 ? dl - SKIP_ONE : 0;
    }
}

static bool msb_push(msb_eval_t *ev, int file) {
    if (ev->nframes >= ev->cap_frames) {
        int ncap = ev->cap_frames ? ev->cap_frames * PAIR_LEN : MSB_INIT;
        msb_frame_t *grown = (msb_frame_t *)ev_alloc(ev, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        if (ev->nframes > 0) {
            memcpy(grown, ev->frames, (size_t)ev->nframes * sizeof(*grown));
            msb_work((size_t)ev->nframes * sizeof(*grown));
        }
        ev->frames = grown;
        ev->cap_frames = ncap;
    }
    ev->frames[ev->nframes++] = (msb_frame_t){.file = file, .group = MSB_TRUE};
    msb_work(sizeof(msb_frame_t));
    return true;
}

static st_rope_t *st_rope_hold(st_rope_t *r) {
    if (r) {
        r->refs++;
        msb_work(1);
    }
    return r;
}
static void st_rope_drop(msb_state_t *s, st_rope_t *r) {
    st_rope_t *pending = NULL;
    if (r && --r->refs == 0) {
        r->next = pending;
        pending = r;
    }
    while (pending) {
        r = pending;
        pending = r->next;
        msb_work(1);
        st_rope_t *children[2] = {r->left, r->right};
        for (int i = 0; i < 2; i++)
            if (children[i] && --children[i]->refs == 0) {
                children[i]->next = pending;
                pending = children[i];
            }
        s->bytes -= sizeof(*r);
        cbm_free(CBM_MEM_CLASS_OTHER, r);
    }
}
static st_rope_t *st_rope_node(msb_eval_t *ev, st_rope_t *a, st_rope_t *b, const msb_item_t *item) {
    if (ev->oom)
        return NULL;
    st_rope_t *r = (st_rope_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*r));
    if (!r) {
        ev->oom = true;
        return NULL;
    }
    ev->state->bytes += sizeof(*r);
    r->refs = 1;
    r->left = st_rope_hold(a);
    r->right = st_rope_hold(b);
    if (item) {
        r->item = *item;
        r->height = 1;
        r->count = 1;
    } else {
        if (a->count > INT_MAX - b->count) {
            ev->oom = true;
            st_rope_drop(ev->state, r);
            return NULL;
        }
        r->height = 1 + (a->height > b->height ? a->height : b->height);
        r->count = a->count + b->count;
    }
    msb_work(sizeof(*r));
    ev_peak(ev);
    return r;
}
static st_rope_t *st_rope_balance(msb_eval_t *ev, st_rope_t *a, st_rope_t *b) {
    if (!a || !b)
        return st_rope_hold(a ? a : b);
    if (a->height > b->height + 1) {
        st_rope_t *right = NULL, *left = NULL, *out = NULL;
        if (a->left->height >= a->right->height) {
            right = st_rope_node(ev, a->right, b, NULL);
            if (right)
                out = st_rope_node(ev, a->left, right, NULL);
        } else {
            left = st_rope_node(ev, a->left, a->right->left, NULL);
            right = st_rope_node(ev, a->right->right, b, NULL);
            if (left && right)
                out = st_rope_node(ev, left, right, NULL);
        }
        st_rope_drop(ev->state, left);
        st_rope_drop(ev->state, right);
        return out;
    }
    if (b->height > a->height + 1) {
        st_rope_t *right = NULL, *left = NULL, *out = NULL;
        if (b->right->height >= b->left->height) {
            left = st_rope_node(ev, a, b->left, NULL);
            if (left)
                out = st_rope_node(ev, left, b->right, NULL);
        } else {
            left = st_rope_node(ev, a, b->left->left, NULL);
            right = st_rope_node(ev, b->left->right, b->right, NULL);
            if (left && right)
                out = st_rope_node(ev, left, right, NULL);
        }
        st_rope_drop(ev->state, left);
        st_rope_drop(ev->state, right);
        return out;
    }
    return st_rope_node(ev, a, b, NULL);
}
/* Join height-balanced nonempty spans. Empty import chains disappear here. */
static st_rope_t *st_rope_join(msb_eval_t *ev, st_rope_t *a, st_rope_t *b) {
    msb_work(1);
    if (!a)
        return st_rope_hold(b);
    if (!b)
        return st_rope_hold(a);
    if (ev->oom)
        return NULL;
    if (a->height > b->height + 1) {
        st_rope_t *tail = st_rope_join(ev, a->right, b);
        st_rope_t *out = tail ? st_rope_balance(ev, a->left, tail) : NULL;
        st_rope_drop(ev->state, tail);
        return out;
    }
    if (b->height > a->height + 1) {
        st_rope_t *head = st_rope_join(ev, a, b->left);
        st_rope_t *out = head ? st_rope_balance(ev, head, b->right) : NULL;
        st_rope_drop(ev->state, head);
        return out;
    }
    return st_rope_node(ev, a, b, NULL);
}
static void st_component_drop(msb_state_t *s, st_component_t *c) {
    if (!c)
        return;
    msb_work(1);
    if (--c->refs)
        return;
    st_view_drop(s, &c->writes);
    st_view_drop(s, &c->inputs);
    st_rope_drop(s, c->items);
    s->bytes -= sizeof(*c);
    cbm_free(CBM_MEM_CLASS_OTHER, c);
}
static bool st_inputs_match(msb_eval_t *ev, const msb_view_t *inputs) {
    for (int d = 0; d < ST_DOMAINS; d++)
        if (!st_validate(ev, inputs->root[d], ev->view.root[d], d))
            return false;
    return true;
}
/* B was selected against the actual after-A state before this call. Preserve
 * every earlier A read, and mask only B's reads by A writes (unknown included). */
static bool st_compose(msb_eval_t *ev, st_component_t *parent, st_component_t *child) {
    if (!parent)
        return true;
    for (int d = 0; d < ST_DOMAINS && !ev->oom; d++) {
        st_node_t *external = st_mask(ev, child->inputs.root[d], parent->writes.root[d], d);
        st_node_t *inputs =
            st_union(ev, parent->inputs.root[d], external, true, d, CBM_MSB_NODE_FAIL_DEPENDENCY);
        st_drop(ev->state, external);
        st_node_t *writes = st_union(ev, child->writes.root[d], parent->writes.root[d], false, d,
                                     CBM_MSB_NODE_FAIL_OVERLAY);
        if (!ev->oom) {
            st_certify(ev, inputs, ev->view.root[d], d);
            st_drop(ev->state, parent->inputs.root[d]);
            parent->inputs.root[d] = inputs;
            st_drop(ev->state, parent->writes.root[d]);
            parent->writes.root[d] = writes;
        } else {
            st_drop(ev->state, inputs);
            st_drop(ev->state, writes);
        }
    }
    if (!ev->oom) {
        st_rope_t *items = st_rope_join(ev, parent->items, child->items);
        if (!ev->oom) {
            st_rope_drop(ev->state, parent->items);
            parent->items = items;
        } else
            st_rope_drop(ev->state, items);
    }
    return !ev->oom;
}
static bool st_apply(msb_eval_t *ev, st_component_t *c) {
    if (!st_compose(ev, ev->builder, c))
        return false;
    cbm_msb_node_fail_operation_t phase =
        ev->builder ? CBM_MSB_NODE_FAIL_OVERLAY : CBM_MSB_NODE_FAIL_APPLY;
    for (int d = 0; d < ST_DOMAINS && !ev->oom; d++) {
        st_node_t *saved_reuse = ev->reuse_nodes;
        ev->reuse_nodes = ev->builder ? ev->builder->writes.root[d] : NULL;
        st_node_t *next = st_union(ev, c->writes.root[d], ev->view.root[d], false, d, phase);
        ev->reuse_nodes = saved_reuse;
        if (!ev->oom) {
            st_drop(ev->state, ev->view.root[d]);
            ev->view.root[d] = next;
        } else
            st_drop(ev->state, next);
    }
    if (ev->oom)
        return false;
    ev->open = ev->open || c->open;
    ev->unevaluable += c->unevaluable;
    ev->outside += c->outside;
    if (!ev->builder) {
        c->refs++;
        ev->completed = c;
    }
    return true;
}
/* Returns true only when an interpreter frame was pushed. A validated cache
 * hit applies the exact effect here, before the caller can mask dependencies. */
static bool st_enter(msb_eval_t *ev, int file, bool poison) {
    int domain = poison ? ST_POISON : ST_SEEN;
    if (st_membership(ev, (uint32_t)file, domain) || ev->oom)
        return false;
    size_t slot = (size_t)file * 2 + (poison ? 1 : 0);
    st_component_t *cached = ev->state->components[slot];
    if (cached && st_inputs_match(ev, &cached->inputs)) {
        (void)st_apply(ev, cached);
        return false;
    }
    st_component_t *c = (st_component_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*c));
    if (!c) {
        ev->oom = true;
        return false;
    }
    c->refs = 1;
    c->parent = ev->builder;
    c->file = file;
    c->poison = poison;
    c->retain = !cached && file != ev->main_project;
    c->start_open = ev->open;
    c->start_unevaluable = ev->unevaluable;
    c->start_outside = ev->outside;
    ev->state->bytes += sizeof(*c);
    msb_work(sizeof(*c));
    ev_peak(ev);
    ev->builder = c;
    ev->open = false;
    ev->unevaluable = 0;
    ev->outside = 0;
    if (!msb_push(ev, file)) {
        ev->oom = true;
        return false;
    }
    ev->frames[ev->nframes - 1].owner = c;
    st_write(ev, (uint32_t)file + 1, domain, NULL);
    /* a file that was not read is counted (and opens the scope) by its 'Y'
     * record, as the walk reaches it */
    return !ev->oom;
}
static void st_finish(msb_eval_t *ev, st_component_t *c) {
    c->open = ev->open;
    c->unevaluable = ev->unevaluable;
    c->outside = ev->outside;
    ev->open = c->start_open || c->open;
    ev->unevaluable += c->start_unevaluable;
    ev->outside += c->start_outside;
    ev->builder = c->parent;
    c->parent = NULL;
    if (!st_compose(ev, ev->builder, c)) {
        st_component_drop(ev->state, c);
        return;
    }
    if (c->retain) {
        size_t slot = (size_t)c->file * 2 + (c->poison ? 1 : 0);
        if (!ev->state->components[slot]) {
            c->refs++;
            ev->state->components[slot] = c;
        }
    }
    if (ev->builder)
        st_component_drop(ev->state, c);
    else
        ev->completed = c;
}
static void st_add_item(msb_eval_t *ev, const msb_rec_t *group, const msb_rec_t *item, int file) {
    msb_item_t it = {.group = group, .item = item, .file = file};
    st_rope_t *leaf = st_rope_node(ev, NULL, NULL, &it);
    st_rope_t *next = leaf ? st_rope_join(ev, ev->builder->items, leaf) : NULL;
    st_rope_drop(ev->state, leaf);
    if (!ev->oom) {
        st_rope_drop(ev->state, ev->builder->items);
        ev->builder->items = next;
    } else
        st_rope_drop(ev->state, next);
}

/* `file` may or may not be imported: what it (and what it imports, under any
 * condition) sets is unknown from here on. */
static void msb_poison(msb_eval_t *ev, int file) {
    int base = ev->nframes;
    if (ev->state) {
        if (!st_enter(ev, file, true))
            return;
    } else {
        if (poisoned_has(ev, ev->m->files[file].rel_path) || !msb_push(ev, file))
            return;
        msb_work(SKIP_ONE);
        cbm_ht_set(ev->poisoned, ev->m->files[file].rel_path, (void *)&MSB_SET_PRESENT);
    }
    while (ev->nframes > base && !ev->oom) {
        msb_frame_t *fr = &ev->frames[ev->nframes - SKIP_ONE];
        const msb_file_t *f = &ev->m->files[fr->file];
        if (fr->rec >= f->nrecs) {
            st_component_t *owner = fr->owner;
            ev->nframes--;
            if (ev->state && owner)
                st_finish(ev, owner);
            continue;
        }
        const msb_rec_t *r = &f->recs[fr->rec++];
        msb_record();
        int at = fr->file;
        if (r->tag == 'V' || r->tag == 'W') {
            msb_set(ev, r->f[1], NULL);
        } else if (r->tag == 'K') {
            msb_set(ev, r->f[0], NULL);
        } else if (r->tag == 'N' || r->tag == 'Y') {
            ev->open = true;
        } else if (r->tag == 'I' && !r->f[3]) {
            int target = msb_import_target(ev, at, r->f[2]);
            if (target >= 0) {
                if (ev->state)
                    (void)st_enter(ev, target, true);
                else if (!poisoned_has(ev, ev->m->files[target].rel_path) && msb_push(ev, target)) {
                    msb_work(SKIP_ONE);
                    cbm_ht_set(ev->poisoned, ev->m->files[target].rel_path,
                               (void *)&MSB_SET_PRESENT);
                }
            }
        }
        cbm_arena_reset(&ev->scratch);
    }
}

static void msb_add_item(msb_eval_t *ev, const msb_rec_t *group, const msb_rec_t *item, int file) {
    if (ev->state) {
        st_add_item(ev, group, item, file);
        return;
    }
    if (ev->nitems >= ev->cap_items) {
        int ncap = ev->cap_items ? ev->cap_items * PAIR_LEN : MSB_INIT;
        msb_item_t *grown = (msb_item_t *)ev_alloc(ev, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return;
        }
        if (ev->nitems > 0) {
            memcpy(grown, ev->items, (size_t)ev->nitems * sizeof(*grown));
            msb_work((size_t)ev->nitems * sizeof(*grown));
        }
        ev->items = grown;
        ev->cap_items = ncap;
    }
    ev->items[ev->nitems++] = (msb_item_t){.group = group, .item = item, .file = file};
    msb_work(sizeof(msb_item_t));
}

/* An <Import> standing in the file on top of the frame stack, under its
 * <ImportGroup>'s condition `group` (MSB_TRUE outside a group). */
static void msb_import(msb_eval_t *ev, int file, const msb_rec_t *r, msb_tri_t group) {
    msb_tri_t c = tri_and(group, msb_cond(ev, file, r->f[1]));
    if (c == MSB_FALSE || r->f[3]) {
        return; /* not taken; or an SDK's file, which is no file of the repository */
    }
    int target = msb_import_target(ev, file, r->f[2]);
    if (c == MSB_UNKNOWN) {
        ev->unevaluable++;
        if (target >= 0) {
            msb_poison(ev, target);
        }
        return;
    }
    if (ev->state) {
        if (target >= 0)
            (void)st_enter(ev, target, false);
        return;
    }
    if (target >= 0 && !seen_has(ev, ev->m->files[target].rel_path) && msb_push(ev, target)) {
        msb_work(SKIP_ONE);
        cbm_ht_set(ev->seen, ev->m->files[target].rel_path, (void *)&MSB_SET_PRESENT);
        /* a file that was not read (malformed, or too large) is counted and
         * opens the scope by its 'Y' record (cbm_msb_add) */
    }
}

/* A property record under its group's condition `group`. */
static void msb_property(msb_eval_t *ev, int file, const msb_rec_t *r, msb_tri_t group) {
    msb_tri_t own = msb_cond(ev, file, r->f[0]);
    msb_tri_t c = tri_and(group, own);
    if (c == MSB_FALSE) {
        return;
    }
    ev->unevaluable += own == MSB_UNKNOWN;
    const char *value =
        (c == MSB_TRUE && r->tag == 'V') ? msb_expand(ev, file, r->f[2] ? r->f[2] : "") : NULL;
    msb_set(ev, r->f[1], value);
}

/* Pass 1 over `file` and what it imports: properties in order, the <Using>
 * items collected for pass 2. No recursion: an import chain is as long as
 * the repository makes it. */
static void msb_pass1(msb_eval_t *ev, int file) {
    int base = ev->nframes;
    const char *rel = ev->m->files[file].rel_path;
    if (ev->state) {
        if (!st_enter(ev, file, false))
            return;
    } else {
        if (seen_has(ev, rel) || !msb_push(ev, file))
            return;
        msb_work(SKIP_ONE);
        cbm_ht_set(ev->seen, rel, (void *)&MSB_SET_PRESENT);
    }
    while (ev->nframes > base && !ev->oom) {
        /* an import pushes a frame, which may move the array: the frame is
         * read into locals before the record is handled */
        msb_frame_t *fr = &ev->frames[ev->nframes - SKIP_ONE];
        const msb_file_t *f = &ev->m->files[fr->file];
        if (fr->rec >= f->nrecs) {
            st_component_t *owner = fr->owner;
            ev->nframes--;
            if (ev->state && owner)
                st_finish(ev, owner);
            continue;
        }
        const msb_rec_t *r = &f->recs[fr->rec++];
        msb_record();
        int at = fr->file;
        switch (r->tag) {
        case 'G':
            fr->group = msb_cond(ev, at, r->f[0]);
            ev->unevaluable += fr->group == MSB_UNKNOWN;
            break;
        case 'V':
        case 'W':
            msb_property(ev, at, r, fr->group);
            break;
        case 'K':
            msb_set(ev, r->f[0], NULL);
            break;
        case 'H':
            fr->hgroup = r;
            break;
        case 'N':
            msb_add_item(ev, fr->hgroup, r, at);
            break;
        case 'Y':
            ev->open = true;
            ev->unevaluable++;
            break;
        case 'C':
            ev->unevaluable++;
            break;
        case 'I': {
            /* the group's condition once per group: its imports share the
             * text (cbm_msb_add); a legacy blob's copies are evaluated each */
            msb_tri_t group = MSB_TRUE;
            if (r->f[0]) {
                if (fr->igroup_text != r->f[0]) {
                    fr->igroup_text = r->f[0];
                    fr->igroup = msb_cond(ev, at, r->f[0]);
                }
                group = fr->igroup;
            }
            msb_import(ev, at, r, group); /* may push a frame: fr is not used after it */
            break;
        }
        default:
            break;
        }
        cbm_arena_reset(&ev->scratch);
    }
}

/* ── Usings ──────────────────────────────────────────────────────── */

typedef struct {
    cbm_msb_using_t *v;
    int n;
    int cap;
    CBMHashTable *unique; /* capture only: exact tuples, or removal targets */
    bool target_only;
} msb_ulist_t;

static void ulist_add(msb_eval_t *ev, msb_ulist_t *l, char kind, const char *alias,
                      const char *target) {
    if (ev->oom) {
        return;
    }
    if (l->n >= l->cap) {
        if (l->cap > INT_MAX / PAIR_LEN) {
            ev->oom = true;
            return;
        }
        int ncap = l->cap ? l->cap * PAIR_LEN : MSB_INIT;
        if ((size_t)ncap > SIZE_MAX / sizeof(cbm_msb_using_t)) {
            ev->oom = true;
            return;
        }
        cbm_msb_using_t *grown = (cbm_msb_using_t *)ev_alloc(ev, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return;
        }
        if (l->n > 0) {
            memcpy(grown, l->v, (size_t)l->n * sizeof(*grown));
            msb_work((size_t)l->n * sizeof(*grown));
        }
        l->v = grown;
        l->cap = ncap;
    }
    l->v[l->n++] = (cbm_msb_using_t){.kind = kind, .alias = alias, .target = target};
    msb_work(sizeof(cbm_msb_using_t));
}

/* The tuple key has a fixed-width hexadecimal target length, followed by
 * the target and alias bytes. It cannot confuse embedded separators or a
 * different target/alias boundary. Only a new tuple acquires owned strings. */
static void ulist_add_unique(msb_eval_t *ev, msb_ulist_t *l, char kind, const char *alias,
                             size_t an, const char *target, size_t tn) {
    size_t header = SKIP_ONE + PAIR_LEN * sizeof(size_t);
    if (tn > SIZE_MAX - header - SKIP_ONE || an > SIZE_MAX - header - tn - SKIP_ONE) {
        ev->oom = true;
        return;
    }
    size_t bytes = l->target_only ? tn : header + tn + an;
    char *key = (char *)scratch_alloc(ev, bytes + SKIP_ONE);
    if (!key) {
        return;
    }
    if (l->target_only) {
        memcpy(key, target, tn);
    } else {
        static const char digits[] = "0123456789abcdef";
        key[0] = kind;
        size_t length = tn;
        for (size_t i = header; i > SKIP_ONE;) {
            key[--i] = digits[length & 15];
            length >>= 4;
        }
        memcpy(key + header, target, tn);
        memcpy(key + header + tn, alias, an);
    }
    key[bytes] = '\0';
    msb_work(bytes + PAIR_LEN); /* key construction and unique lookup */
    if (cbm_ht_get(l->unique, key)) {
        return;
    }
    char *owned_key = ev_strndup(ev, key, bytes);
    const char *owned_target = l->target_only ? owned_key : ev_strndup(ev, target, tn);
    const char *owned_alias = an ? ev_strndup(ev, alias, an) : "";
    if (ev->oom) {
        return;
    }
    msb_work(SKIP_ONE);
    if (!msb_fail_item_operation(CBM_MSB_ITEM_FAIL_UNIQUE_INSERT)) {
        msb_work(SKIP_ONE);
        cbm_ht_set(l->unique, owned_key, owned_key);
    }
    if (cbm_ht_get(l->unique, owned_key) != owned_key) {
        ev->oom = true;
        return;
    }
    if (!l->target_only) {
        ulist_add(ev, l, kind, owned_alias, owned_target);
    }
}

/* Add every ';'-separated entry. During capture, neither aliases nor targets
 * are copied into persistent storage before exact duplicate detection. */
static void ulist_add_split(msb_eval_t *ev, msb_ulist_t *l, char kind, const char *alias, size_t an,
                            const char *list) {
    if (!l->unique && an > 0) {
        alias = ev_strndup(ev, alias, an);
    }
    for (const char *p = list; !ev->oom && p;) {
        const char *e = strchr(p, ';');
        size_t n = e ? (size_t)(e - p) : strlen(p);
        const char *s = p;
        trim_span(&s, &n);
        if (n > 0) {
            if (l->unique) {
                ulist_add_unique(ev, l, kind, alias, an, s, n);
            } else {
                ulist_add(ev, l, kind, an ? alias : "", ev_strndup(ev, s, n));
            }
        }
        p = e ? e + SKIP_ONE : NULL;
    }
}

static int using_cmp(const void *a, const void *b) {
    msb_work(SKIP_ONE);
    const cbm_msb_using_t *x = (const cbm_msb_using_t *)a;
    const cbm_msb_using_t *y = (const cbm_msb_using_t *)b;
    if (x->kind != y->kind) {
        return x->kind < y->kind ? -1 : 1;
    }
    int c = strcmp(x->target, y->target);
    return c ? c : strcmp(x->alias, y->alias);
}

static int target_cmp(const void *a, const void *b) {
    msb_work(SKIP_ONE);
    return strcmp(((const cbm_msb_using_t *)a)->target, ((const cbm_msb_using_t *)b)->target);
}

/* An <ItemGroup>'s condition for the items of one pass over them. MSBuild
 * evaluates it once, where the group stands, and the final properties the
 * items see do not change while they are read: evaluating it again for
 * every item cost items x condition length. */
typedef struct {
    const msb_rec_t *group;
    msb_tri_t value;
} msb_group_memo_t;

static msb_tri_t item_group_cond(msb_eval_t *ev, const msb_item_t *it, msb_group_memo_t *memo) {
    if (!it->group) {
        return MSB_TRUE;
    }
    if (memo->group != it->group) {
        memo->group = it->group;
        memo->value = msb_cond(ev, it->file, it->group->f[0]);
    }
    return memo->value;
}

/* One <Using> item with the final properties: into `inc` or `rem`. */
static void msb_using(msb_eval_t *ev, const msb_item_t *it, msb_ulist_t *inc, msb_ulist_t *rem,
                      msb_group_memo_t *memo) {
    const msb_rec_t *r = it->item;
    msb_record();
    msb_tri_t c = tri_and(item_group_cond(ev, it, memo), msb_cond(ev, it->file, r->f[0]));
    if (c == MSB_FALSE) {
        return;
    }
    const char *include = r->f[1] ? msb_expand(ev, it->file, r->f[1]) : "";
    const char *remove = r->f[2] ? msb_expand(ev, it->file, r->f[2]) : "";
    const char *is_static = r->f[3] ? msb_expand(ev, it->file, r->f[3]) : "";
    const char *alias = r->f[4] ? msb_expand(ev, it->file, r->f[4]) : "";
    if (c == MSB_UNKNOWN || !include || !remove || !is_static || !alias) {
        ev->open = true;
        ev->unevaluable++;
        return;
    }
    size_t sn = strlen(is_static);
    trim_span(&is_static, &sn);
    size_t an = strlen(alias);
    trim_span(&alias, &an);
    char kind = an > 0 ? 'a' : (ci_is(is_static, sn, "true") ? 's' : 'n');
    ulist_add_split(ev, inc, kind, alias, an, include);
    ulist_add_split(ev, rem, 'n', "", 0, remove);
}

static const char *const SDK_DEFAULT[] = {
    "System",           "System.Collections.Generic", "System.IO", "System.Linq", "System.Net.Http",
    "System.Threading", "System.Threading.Tasks",     NULL};
static const char *const SDK_WEB[] = {"System",
                                      "System.Collections.Generic",
                                      "System.IO",
                                      "System.Linq",
                                      "System.Net.Http",
                                      "System.Net.Http.Json",
                                      "System.Threading",
                                      "System.Threading.Tasks",
                                      "Microsoft.AspNetCore.Builder",
                                      "Microsoft.AspNetCore.Hosting",
                                      "Microsoft.AspNetCore.Http",
                                      "Microsoft.AspNetCore.Routing",
                                      "Microsoft.Extensions.Configuration",
                                      "Microsoft.Extensions.DependencyInjection",
                                      "Microsoft.Extensions.Hosting",
                                      "Microsoft.Extensions.Logging",
                                      NULL};
static const char *const SDK_WORKER[] = {"System",
                                         "System.Collections.Generic",
                                         "System.IO",
                                         "System.Linq",
                                         "System.Net.Http",
                                         "System.Threading",
                                         "System.Threading.Tasks",
                                         "Microsoft.Extensions.Configuration",
                                         "Microsoft.Extensions.DependencyInjection",
                                         "Microsoft.Extensions.Hosting",
                                         "Microsoft.Extensions.Logging",
                                         NULL};

/* True when the ';'-separated SDK list names `sdk` (a version after '/' is
 * no part of the name). */
static bool sdk_listed(const char *list, const char *sdk) {
    size_t sl = strlen(sdk);
    for (const char *p = list; p;) {
        const char *e = strchr(p, ';');
        size_t n = e ? (size_t)(e - p) : strlen(p);
        const char *s = p;
        trim_span(&s, &n);
        const char *slash = memchr(s, '/', n);
        size_t name = slash ? (size_t)(slash - s) : n;
        if (name == sl && memcmp(s, sdk, sl) == 0) {
            return true;
        }
        p = e ? e + SKIP_ONE : NULL;
    }
    return false;
}

/* The SDK a project file names: its <Project Sdk> attribute, else the first
 * import that names one. NULL when it names none. */
static const char *msb_sdk(const msb_file_t *f) {
    const char *sdk = f->sdk;
    for (int i = 0; !sdk && i < f->nrecs; i++) {
        sdk = f->recs[i].tag == 'I' ? f->recs[i].f[3] : NULL;
    }
    return sdk;
}

bool cbm_msb_compiles(const cbm_msb_t *m, const char *rel_path) {
    /* These two SDKs are defined as producing no assembly (one runs build
     * steps, the other builds other projects): that is why they are named. */
    static const char *const idle[] = {"Microsoft.Build.NoTargets", "Microsoft.Build.Traversal"};
    int at = (m && rel_path) ? msb_file_index(m, rel_path) : CBM_NOT_FOUND;
    const char *sdk = at >= 0 ? msb_sdk(&m->files[at]) : NULL;
    for (size_t i = 0; sdk && i < sizeof(idle) / sizeof(idle[0]); i++) {
        if (sdk_listed(sdk, idle[i])) {
            return false;
        }
    }
    return true;
}

/* The usings ImplicitUsings brings in, or NULL. An ImplicitUsings that some
 * file sets to a value this reader does not know leaves the usings open. */
static const char *const *msb_implicit(msb_eval_t *ev, int project) {
    static const char name[] = "ImplicitUsings";
    char key[MSB_NAME_MAX];
    (void)msb_key(name, sizeof(name) - SKIP_ONE, key);
    const msb_prop_t *p = prop_lookup(ev, key);
    if (!p) {
        return NULL;
    }
    msb_tri_t on =
        p->value ? tri_or(msb_equal(p->value, "enable"), msb_equal(p->value, "true")) : MSB_UNKNOWN;
    if (on == MSB_UNKNOWN) {
        ev->open = true;
        ev->unevaluable++;
    }
    if (on != MSB_TRUE) {
        return NULL;
    }
    const char *sdk = msb_sdk(&ev->m->files[project]);
    if (sdk && sdk_listed(sdk, "Microsoft.NET.Sdk.Web")) {
        return SDK_WEB;
    }
    return (sdk && sdk_listed(sdk, "Microsoft.NET.Sdk.Worker")) ? SDK_WORKER : SDK_DEFAULT;
}

/* The evaluation's usings as one heap block: the array, then its strings. */
static bool msb_result(msb_eval_t *ev, msb_ulist_t *inc, const msb_ulist_t *rem,
                       cbm_msb_result_t *out) {
    if (rem->n > 0) {
        qsort(rem->v, (size_t)rem->n, sizeof(rem->v[0]), target_cmp);
    }
    if (inc->n > 0) {
        qsort(inc->v, (size_t)inc->n, sizeof(inc->v[0]), using_cmp);
    }
    int kept = 0;
    size_t bytes = 0;
    for (int i = 0; i < inc->n; i++) {
        const cbm_msb_using_t *u = &inc->v[i];
        bool removed =
            rem->n > 0 && bsearch(u, rem->v, (size_t)rem->n, sizeof(rem->v[0]), target_cmp) != NULL;
        if (!removed && ev->shared_removals) {
            msb_work(SKIP_ONE);
            removed = cbm_ht_get(ev->shared_removals, u->target) != NULL;
        }
        bool dup = kept > 0 && using_cmp(&inc->v[kept - SKIP_ONE], u) == 0;
        if (removed || dup) {
            continue;
        }
        inc->v[kept++] = *u;
        msb_work(sizeof(*u));
        bytes += strlen(u->alias) + strlen(u->target) + PAIR_LEN;
    }
    out->open = ev->open;
    out->unevaluable = ev->unevaluable;
    out->outside = ev->outside;
    if (kept == 0) {
        return !ev->oom;
    }
    size_t head = (size_t)kept * sizeof(cbm_msb_using_t);
    char *mem = msb_fail_item_operation(CBM_MSB_ITEM_FAIL_PUBLISH_ALLOC)
                    ? NULL
                    : (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, head + bytes);
    if (!mem) {
        return false;
    }
    ev->result_bytes = head + bytes;
    ev_peak(ev);
    cbm_msb_using_t *arr = (cbm_msb_using_t *)(void *)mem;
    char *w = mem + head;
    for (int i = 0; i < kept; i++) {
        size_t al = strlen(inc->v[i].alias) + SKIP_ONE;
        size_t tl = strlen(inc->v[i].target) + SKIP_ONE;
        memcpy(w, inc->v[i].alias, al);
        memcpy(w + al, inc->v[i].target, tl);
        arr[i] = (cbm_msb_using_t){.kind = inc->v[i].kind, .alias = w, .target = w + al};
        msb_work(al + tl + sizeof(arr[i]));
        w += al + tl;
    }
    out->usings = arr;
    out->count = kept;
    out->mem = mem;
    return !ev->oom;
}

/* The properties MSBuild sets before it reads the project file. */
static void msb_seed(msb_eval_t *ev, int project) {
    const msb_file_t *f = &ev->m->files[project];
    msb_set(ev, "MSBuildProjectName", f->stem);
    msb_set(ev, "MSBuildProjectFile", f->name);
    msb_set(ev, "MSBuildProjectExtension", f->ext);
    msb_set(ev, "MSBuildProjectFullPath", f->abs_path);
    /* the directory without its trailing '/' */
    size_t dl = strlen(f->abs_dir);
    char *dir = scratch_strndup(ev, f->abs_dir, dl > SKIP_ONE ? dl - SKIP_ONE : dl);
    if (dir) {
        msb_set(ev, "MSBuildProjectDirectory", dir);
    }
}

static bool eval_init(msb_eval_t *ev) {
    cbm_arena_init(&ev->arena);
    cbm_arena_init(&ev->scratch);
    ev_peak(ev);
    ev->props = cbm_ht_create(CBM_SZ_64);
    ev->seen = cbm_ht_create(MSB_INIT);
    ev->poisoned = cbm_ht_create(MSB_INIT);
    ev->oom = !ev->props || !ev->seen || !ev->poisoned;
    return !ev->oom;
}

static void eval_clear(msb_eval_t *ev) {
    if (ev->state)
        st_view_drop(ev->state, &ev->state_inputs);
    if (ev->props) {
        cbm_ht_foreach(ev->props, prop_clear, ev);
    }
    cbm_ht_free(ev->props);
    cbm_ht_free(ev->seen);
    cbm_ht_free(ev->poisoned);
    cbm_ht_free(ev->inputs.props);
    cbm_ht_free(ev->inputs.seen);
    cbm_ht_free(ev->inputs.poisoned);
    cbm_arena_destroy(&ev->arena);
    cbm_arena_destroy(&ev->scratch);
}

bool cbm_msb_eval(const cbm_msb_t *m, const char *project_rel, cbm_msb_result_t *out) {
    memset(out, 0, sizeof(*out));
    int project = (m && project_rel) ? msb_file_index(m, project_rel) : CBM_NOT_FOUND;
    if (project < 0) {
        return false;
    }
    msb_eval_t ev = {.m = m};
    cbm_arena_init(&ev.arena);
    cbm_arena_init(&ev.scratch);
    ev_peak(&ev);
    ev.props = cbm_ht_create(CBM_SZ_64);
    ev.seen = cbm_ht_create(MSB_INIT);
    ev.poisoned = cbm_ht_create(MSB_INIT);
    bool ok = ev.props && ev.seen && ev.poisoned;
    if (ok) {
        const msb_file_t *f = &m->files[project];
        msb_seed(&ev, project);
        cbm_arena_reset(&ev.scratch);
        int props = msb_nearest(&ev, f->dir, "Directory.Build.props");
        cbm_arena_reset(&ev.scratch);
        if (props >= 0) {
            msb_pass1(&ev, props);
        }
        msb_pass1(&ev, project);
        int targets = msb_nearest(&ev, f->dir, "Directory.Build.targets");
        cbm_arena_reset(&ev.scratch);
        if (targets >= 0) {
            msb_pass1(&ev, targets);
        }
        msb_ulist_t inc = {0};
        msb_ulist_t rem = {0};
        const char *const *implicit = msb_implicit(&ev, project);
        for (int i = 0; implicit && implicit[i]; i++) {
            ulist_add(&ev, &inc, 'n', "", implicit[i]);
        }
        msb_group_memo_t memo = {0};
        for (int i = 0; i < ev.nitems; i++) {
            msb_using(&ev, &ev.items[i], &inc, &rem, &memo);
            /* All four expansions remain live until aliases and targets
             * have been copied into the persistent evaluation arena. */
            cbm_arena_reset(&ev.scratch);
        }
        ok = !ev.oom && msb_result(&ev, &inc, &rem, out);
    }
    eval_clear(&ev);
    if (!ok) {
        cbm_msb_result_free(out);
    }
    return ok;
}

/* One immutable prefix and one exact target effect. The target effect's
 * dependency keys are indexed, so validation walks CURRENT local overrides,
 * not every property or imported file in a large shared closure. */
typedef struct {
    bool known[PAIR_LEN];
    int file[PAIR_LEN];
} msb_nearest_entry_t;

struct cbm_msb_eval_context {
    const cbm_msb_t *m;
    msb_state_t *state;
    uint64_t generation;
    int root;
    bool ready;
    msb_eval_t prefix;
    int target_root;
    const msb_eval_t *target_base;
    bool target_live;
    bool target_ready;
    msb_eval_t target;
    bool items_live;
    bool items_ready;
    msb_eval_t item_owner; /* lazy arena/dependencies; no property or import state */
    msb_ulist_t item_includes;
    msb_ulist_t item_removals;
    CBMHashTable *nearest; /* immutable model-owned directory -> two nearest-file results */
    size_t nearest_bytes;
};

static void nearest_entry_free(const char *key, void *value, void *userdata) {
    (void)key;
    cbm_msb_eval_context_t *context = (cbm_msb_eval_context_t *)userdata;
    cbm_free(CBM_MEM_CLASS_OTHER, value);
    context->nearest_bytes -= sizeof(msb_nearest_entry_t);
    msb_work(SKIP_ONE);
}

static void context_drop_items(cbm_msb_eval_context_t *context) {
    if (context->items_live) {
        msb_work(SKIP_ONE + context->item_owner.arena.nblocks +
                 context->item_owner.scratch.nblocks);
        cbm_ht_free(context->item_includes.unique);
        cbm_ht_free(context->item_removals.unique);
        eval_clear(&context->item_owner);
        memset(&context->item_owner, 0, sizeof(context->item_owner));
        context->item_includes = (msb_ulist_t){0};
        context->item_removals = (msb_ulist_t){0};
        msb_work(sizeof(context->item_owner) + sizeof(msb_ulist_t) * PAIR_LEN);
        context->items_live = false;
        context->items_ready = false;
    }
}

static void context_drop_target(cbm_msb_eval_context_t *context) {
    if (context->target_live) {
        context_drop_items(context);
        eval_clear(&context->target);
        memset(&context->target, 0, sizeof(context->target));
        msb_work(sizeof(context->target));
        context->target_live = false;
        context->target_ready = false;
        context->target_base = NULL;
    }
}

static void context_drop_prefix(cbm_msb_eval_context_t *context) {
    if (context->ready) {
        context_drop_items(context);
        /* Target dependencies can borrow the immutable prefix identity. */
        context_drop_target(context);
        eval_clear(&context->prefix);
        memset(&context->prefix, 0, sizeof(context->prefix));
        msb_work(sizeof(context->prefix));
        context->ready = false;
    }
}

static void context_clear_nearest(cbm_msb_eval_context_t *context) {
    cbm_ht_foreach(context->nearest, nearest_entry_free, context);
    cbm_ht_clear(context->nearest);
    msb_work(SKIP_ONE);
}

static msb_state_t *st_state_new(const cbm_msb_t *m);
static void st_state_free(msb_state_t *s);
static bool st_context_eval(cbm_msb_eval_context_t *context, const char *project_rel,
                            cbm_msb_result_t *out);

cbm_msb_eval_context_t *cbm_msb_eval_context_new(const cbm_msb_t *m) {
    if (!m) {
        return NULL;
    }
    cbm_msb_eval_context_t *context =
        (cbm_msb_eval_context_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*context));
    if (!context) {
        return NULL;
    }
    msb_work(sizeof(*context));
    context->m = m;
    context->generation = m->generation;
    context->root = CBM_NOT_FOUND;
    context->target_root = CBM_NOT_FOUND;
    context->nearest = cbm_ht_create(MSB_INIT);
    context->state = st_state_new(m);
    if (!context->nearest || !context->state) {
        st_state_free(context->state);
        cbm_ht_free(context->nearest);
        cbm_free(CBM_MEM_CLASS_OTHER, context);
        return NULL;
    }
    return context;
}

void cbm_msb_eval_context_free(cbm_msb_eval_context_t *context) {
    if (context) {
        context_drop_items(context);
        context_drop_target(context);
        context_drop_prefix(context);
        st_state_free(context->state);
        cbm_ht_foreach(context->nearest, nearest_entry_free, context);
        cbm_ht_free(context->nearest);
        cbm_free(CBM_MEM_CLASS_OTHER, context);
    }
}

/* Cache metadata only. Directory keys live in the model, and an absent
 * result is as reusable as a present one until the model generation changes. */
static int context_nearest(cbm_msb_eval_context_t *context, msb_eval_t *ev, const char *dir,
                           bool targets) {
    msb_work(SKIP_ONE);
    msb_nearest_entry_t *entry = (msb_nearest_entry_t *)cbm_ht_get(context->nearest, dir);
    if (!entry) {
        if (sizeof(*entry) > SIZE_MAX - context->nearest_bytes) {
            ev->oom = true;
            return CBM_NOT_FOUND;
        }
        entry = (msb_nearest_entry_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*entry));
        if (!entry) {
            ev->oom = true;
            return CBM_NOT_FOUND;
        }
        context->nearest_bytes += sizeof(*entry);
        ev_peak(ev);
        memset(entry, 0, sizeof(*entry));
        msb_work(sizeof(*entry) + PAIR_LEN);
        cbm_ht_set(context->nearest, dir, entry);
        if (cbm_ht_get(context->nearest, dir) != entry) {
            nearest_entry_free(dir, entry, context);
            ev->oom = true;
            return CBM_NOT_FOUND;
        }
    }
    int which = targets ? SKIP_ONE : 0;
    msb_work(SKIP_ONE);
    if (!entry->known[which]) {
        int found =
            msb_nearest(ev, dir, targets ? "Directory.Build.targets" : "Directory.Build.props");
        cbm_arena_reset(&ev->scratch);
        if (ev->oom) {
            return CBM_NOT_FOUND;
        }
        entry->file[which] = found;
        entry->known[which] = true;
    }
    return entry->file[which];
}

static void attach_prefix(msb_eval_t *ev, const msb_eval_t *prefix) {
    ev->base = prefix;
    ev->open = prefix->open;
    ev->unevaluable = prefix->unevaluable;
    ev->outside = prefix->outside;
    msb_work(SKIP_ONE);
    ev_peak(ev);
}

typedef struct {
    const msb_inputs_t *inputs;
    const msb_eval_t *effects; /* final-item inputs: target writes hide project entries */
    size_t props;
    size_t seen;
    size_t poisoned;
    bool match;
} msb_input_check_t;

static void check_prop_input(const char *key, void *value, void *userdata) {
    msb_input_check_t *check = (msb_input_check_t *)userdata;
    msb_work(SKIP_ONE); /* every CURRENT local entry visited, including irrelevant ones */
    if (!check->match) {
        return;
    }
    if (check->effects) {
        msb_work(SKIP_ONE);
        if (cbm_ht_get(check->effects->props, key)) {
            return;
        }
    }
    msb_work(SKIP_ONE);
    const msb_prop_dep_t *dep = (const msb_prop_dep_t *)cbm_ht_get(check->inputs->props, key);
    if (dep) {
        check->match = prop_dep_matches(dep, (const msb_prop_t *)value);
        check->props += check->match && dep->exception;
    }
}

static void check_set_input(const char *key, msb_input_check_t *check, bool poison) {
    msb_work(SKIP_ONE);
    if (!check->match) {
        return;
    }
    msb_work(SKIP_ONE);
    const msb_set_dep_t *dep = (const msb_set_dep_t *)cbm_ht_get(
        poison ? check->inputs->poisoned : check->inputs->seen, key);
    if (dep) {
        check->match = dep->present;
        if (poison) {
            check->poisoned += check->match && dep->exception;
        } else {
            check->seen += check->match && dep->exception;
        }
    }
}

static void check_seen_input(const char *key, void *value, void *userdata) {
    (void)value;
    check_set_input(key, (msb_input_check_t *)userdata, false);
}

static void check_poisoned_input(const char *key, void *value, void *userdata) {
    (void)value;
    check_set_input(key, (msb_input_check_t *)userdata, true);
}

static bool seed_inputs_match(const msb_inputs_t *inputs, msb_eval_t *incoming) {
    for (int i = 0; i < MSB_SEEDS; i++) {
        msb_work(SKIP_ONE);
        const msb_prop_dep_t *dep = inputs->seeds[i];
        if (dep && !prop_dep_matches(dep, prop_lookup(incoming, MSB_SEED_NAMES[i]))) {
            return false;
        }
    }
    return true;
}

/* A dependency equal to the immutable baseline needs no local override.
 * Every other dependency needs a matching CURRENT local entry. Counting
 * matched exceptions catches missing inputs without scanning cached maps. */
static bool target_inputs_match(const msb_eval_t *target, msb_eval_t *incoming) {
    msb_input_check_t check = {.inputs = &target->inputs, .match = true};
    cbm_ht_foreach(incoming->props, check_prop_input, &check);
    cbm_ht_foreach(incoming->seen, check_seen_input, &check);
    cbm_ht_foreach(incoming->poisoned, check_poisoned_input, &check);
    if (!check.match || check.props != target->inputs.prop_exceptions ||
        check.seen != target->inputs.seen_exceptions ||
        check.poisoned != target->inputs.poisoned_exceptions) {
        return false;
    }
    return seed_inputs_match(&target->inputs, incoming);
}

static bool apply_targets(cbm_msb_eval_context_t *context, msb_eval_t *ev, int root) {
    if (root < 0) {
        context_drop_target(context);
        return !ev->oom;
    }
    msb_work(SKIP_ONE);
    bool hit = context->target_ready && context->target_root == root &&
               context->target_base == ev->base && target_inputs_match(&context->target, ev);
    if (!hit) {
        context_drop_items(context);
        context_drop_target(context);
        msb_eval_t *target = &context->target;
        target->m = ev->m;
        target->incoming = ev;
        target->live = ev->live;
        target->capture_inputs = true;
        context->target_live = true;
        bool ok = eval_init(target);
        target->inputs.props = cbm_ht_create(MSB_INIT);
        target->inputs.seen = cbm_ht_create(MSB_INIT);
        target->inputs.poisoned = cbm_ht_create(MSB_INIT);
        target->oom = target->oom || !target->inputs.props || !target->inputs.seen ||
                      !target->inputs.poisoned;
        if (ok && !target->oom) {
            msb_pass1(target, root);
        }
        if (target->oom || target->nframes) {
            ev->oom = true;
            context_drop_target(context);
            return false;
        }
        target->incoming = NULL;
        target->live = NULL;
        target->capture_inputs = false;
        context->target_root = root;
        context->target_base = ev->base;
        context->target_ready = true;
    }
    ev->effects = &context->target;
    ev->open = ev->open || context->target.open;
    ev->unevaluable += context->target.unevaluable;
    ev->outside += context->target.outside;
    msb_work(SKIP_ONE);
    ev_peak(ev);
    return true;
}

static void eval_items(msb_eval_t *ev, const msb_eval_t *owner, msb_ulist_t *inc,
                       msb_ulist_t *rem) {
    msb_group_memo_t memo = {0};
    for (int i = 0; !ev->oom && i < owner->nitems; i++) {
        msb_using(ev, &owner->items[i], inc, rem, &memo);
        cbm_arena_reset(&ev->scratch);
    }
}

/* Item dependencies see final properties. A present target value, even
 * unknown, hides a project override. Matching exception counts detect inputs
 * missing from CURRENT locals without walking the retained dependency map. */
static bool item_inputs_match(const msb_eval_t *items, msb_eval_t *incoming) {
    if (incoming->state)
        return st_inputs_match(incoming, &items->state_inputs);
    msb_input_check_t check = {
        .inputs = &items->inputs, .effects = incoming->effects, .match = true};
    cbm_ht_foreach(incoming->props, check_prop_input, &check);
    return check.match && check.props == items->inputs.prop_exceptions &&
           seed_inputs_match(&items->inputs, incoming);
}

static void st_eval_rope(msb_eval_t *ev, st_rope_t *rope, msb_ulist_t *inc, msb_ulist_t *rem);

static bool capture_items(cbm_msb_eval_context_t *context, msb_eval_t *ev) {
    msb_eval_t *items = &context->item_owner;
    items->m = ev->m;
    items->incoming = ev;
    items->live = ev->live;
    items->capture_inputs = true;
    if (ev->state) {
        items->state = ev->state;
        items->view = ev->view;
    }
    items->item_alloc_operation = CBM_MSB_ITEM_FAIL_CAPTURE_ALLOC;
    context->items_live = true;
    cbm_arena_init_lazy(&items->arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
    cbm_arena_init_lazy(&items->scratch, CBM_ARENA_DEFAULT_BLOCK_SIZE);
    items->inputs.props = cbm_ht_create(MSB_INIT);
    context->item_includes.unique = cbm_ht_create(MSB_INIT);
    context->item_removals.unique = cbm_ht_create(MSB_INIT);
    context->item_removals.target_only = true;
    items->oom =
        !items->inputs.props || !context->item_includes.unique || !context->item_removals.unique;
    msb_work(sizeof(*items));
    if (ev->state) {
        st_eval_rope(items, ev->state->capture_a, &context->item_includes, &context->item_removals);
        st_eval_rope(items, ev->state->capture_b, &context->item_includes, &context->item_removals);
    } else {
        if (!items->oom && ev->base)
            eval_items(items, ev->base, &context->item_includes, &context->item_removals);
        if (!items->oom && ev->effects)
            eval_items(items, ev->effects, &context->item_includes, &context->item_removals);
    }
    if (items->oom) {
        ev->oom = true;
        context_drop_items(context);
        return false;
    }
    /* Shared removals apply globally. Prune shared includes once, but retain
     * the removal index for project and implicit includes at publication. */
    msb_ulist_t *inc = &context->item_includes;
    int kept = 0;
    for (int i = 0; i < inc->n; i++) {
        msb_work(SKIP_ONE);
        if (!cbm_ht_get(context->item_removals.unique, inc->v[i].target)) {
            inc->v[kept++] = inc->v[i];
            msb_work(sizeof(inc->v[i]));
        }
    }
    inc->n = kept;
    cbm_ht_free(inc->unique);
    inc->unique = NULL;
    msb_work(SKIP_ONE + items->scratch.nblocks);
    cbm_arena_destroy(&items->scratch);
    items->incoming = NULL;
    items->view = (msb_view_t){0};
    items->live = NULL;
    items->capture_inputs = false;
    items->item_alloc_operation = CBM_MSB_ITEM_FAIL_NONE;
    context->items_ready = true;
    return true;
}

static bool apply_items(cbm_msb_eval_context_t *context, msb_eval_t *ev, msb_ulist_t *inc,
                        msb_ulist_t *rem) {
    if (ev->state) {
        if (!ev->state->capture_a && !ev->state->capture_b)
            return !ev->oom;
    } else if ((!ev->base || !ev->base->nitems) && (!ev->effects || !ev->effects->nitems)) {
        return !ev->oom;
    }
    msb_work(SKIP_ONE);
    if (context->items_ready && !item_inputs_match(&context->item_owner, ev)) {
        /* Keep the first exact variant for this owner epoch. A different
         * project's final inputs do not churn retained item storage. */
        if (ev->state) {
            st_eval_rope(ev, ev->state->capture_a, inc, rem);
            st_eval_rope(ev, ev->state->capture_b, inc, rem);
        } else {
            if (ev->base)
                eval_items(ev, ev->base, inc, rem);
            if (ev->effects)
                eval_items(ev, ev->effects, inc, rem);
        }
        return !ev->oom;
    }
    if (!context->items_ready && !capture_items(context, ev)) {
        return false;
    }
    const msb_ulist_t *shared = &context->item_includes;
    if (shared->n > 0) {
        if (inc->n > INT_MAX - shared->n ||
            (size_t)(inc->n + shared->n) > SIZE_MAX / sizeof(*inc->v)) {
            ev->oom = true;
            return false;
        }
        int n = inc->n + shared->n;
        cbm_msb_item_fail_operation_t previous = ev->item_alloc_operation;
        ev->item_alloc_operation = CBM_MSB_ITEM_FAIL_APPLY_ALLOC;
        cbm_msb_using_t *v = (cbm_msb_using_t *)ev_alloc(ev, (size_t)n * sizeof(*v));
        ev->item_alloc_operation = previous;
        if (!v) {
            return false;
        }
        if (inc->n) {
            memcpy(v, inc->v, (size_t)inc->n * sizeof(*v));
        }
        memcpy(v + inc->n, shared->v, (size_t)shared->n * sizeof(*v));
        msb_work((size_t)n * sizeof(*v));
        inc->v = v;
        inc->n = n;
        inc->cap = n;
    }
    ev->shared_removals = context->item_removals.unique;
    ev->open = ev->open || context->item_owner.open;
    ev->unevaluable += context->item_owner.unevaluable;
    ev->outside += context->item_owner.outside;
    msb_work(SKIP_ONE);
    ev_peak(ev);
    return !ev->oom;
}

bool cbm_msb_eval_context_eval(cbm_msb_eval_context_t *context, const char *project_rel,
                               cbm_msb_result_t *out) {
    if (context && context->state)
        return st_context_eval(context, project_rel, out);
    memset(out, 0, sizeof(*out));
    if (!context) {
        return false;
    }
    const cbm_msb_t *m = context->m;
    msb_work(SKIP_ONE);
    if (context->generation != m->generation) {
        context_drop_items(context);
        context_drop_target(context);
        context_drop_prefix(context);
        context_clear_nearest(context);
        context->generation = m->generation;
    }
    int project = project_rel ? msb_file_index(m, project_rel) : CBM_NOT_FOUND;
    if (project < 0) {
        return false;
    }
    msb_eval_t seeds = {.m = m};
    msb_eval_t ev = {.m = m, .seeds = &seeds, .base = context->ready ? &context->prefix : NULL};
    msb_live_t live = {
        .owners = {&context->prefix, &context->target, &ev, &seeds, &context->item_owner},
        .metadata_bytes = &context->nearest_bytes};
    ev.live = &live;
    seeds.live = &live;
    bool ok = eval_init(&ev);
    bool seeds_ok = eval_init(&seeds);
    if (ok && seeds_ok) {
        msb_seed(&seeds, project);
        cbm_arena_reset(&seeds.scratch);
        ok = !seeds.oom;
    } else {
        ok = false;
    }
    if (ok) {
        const msb_file_t *f = &m->files[project];
        int props = context_nearest(context, &ev, f->dir, false);
        msb_work(SKIP_ONE);
        if (context->ready && context->root == props) {
            attach_prefix(&ev, &context->prefix);
        } else {
            ev.base = NULL;
            /* No retained prefix means a stable empty baseline. In
             * particular, no-props calls must not discard target effects. */
            context_drop_prefix(context);
            ev.track_seed_reads = true;
            if (props >= 0) {
                msb_pass1(&ev, props);
            }
            ev.track_seed_reads = false;
            if (props >= 0 && !ev.oom && !ev.seed_read && ev.nframes == 0) {
                context_drop_items(context);
                context_drop_target(context);
                context->prefix = ev;
                msb_work(sizeof(ev));
                context->prefix.seeds = NULL;
                context->prefix.base = NULL;
                context->prefix.live = NULL;
                context->root = props;
                context->ready = true;
                memset(&ev, 0, sizeof(ev));
                msb_work(sizeof(ev));
                ev.m = m;
                ev.seeds = &seeds;
                ev.base = &context->prefix;
                ev.live = &live;
                ok = eval_init(&ev);
                attach_prefix(&ev, &context->prefix);
            }
        }
        if (ok && !ev.oom) {
            msb_pass1(&ev, project);
            int targets = context_nearest(context, &ev, f->dir, true);
            ok = !ev.oom && apply_targets(context, &ev, targets);
            if (ok) {
                msb_ulist_t inc = {0};
                msb_ulist_t rem = {0};
                const char *const *implicit = msb_implicit(&ev, project);
                for (int i = 0; !ev.oom && implicit && implicit[i]; i++) {
                    ulist_add(&ev, &inc, 'n', "", implicit[i]);
                }
                ok = !ev.oom && apply_items(context, &ev, &inc, &rem);
                if (ok) {
                    eval_items(&ev, &ev, &inc, &rem);
                    ok = !ev.oom && msb_result(&ev, &inc, &rem, out);
                }
            }
        } else {
            ok = false;
        }
    }
    eval_clear(&ev);
    eval_clear(&seeds);
    if (!ok) {
        cbm_msb_result_free(out);
    }
    return ok;
}

/* Traverse source spans only. Empty chains collapse at construction; the
 * bounded explicit stack follows the balanced rope, not the import graph. */
static void st_eval_rope(msb_eval_t *ev, st_rope_t *rope, msb_ulist_t *inc, msb_ulist_t *rem) {
    st_rope_t *stack[CBM_SZ_64];
    int depth = 0;
    msb_group_memo_t memo = {0};
    while (!ev->oom && (rope || depth)) {
        if (!rope) {
            rope = stack[--depth];
            continue;
        }
        msb_work(1);
        if (!rope->left) {
            msb_using(ev, &rope->item, inc, rem, &memo);
            cbm_arena_reset(&ev->scratch);
            rope = NULL;
        } else {
            if (depth == CBM_SZ_64) {
                ev->oom = true;
                break;
            }
            stack[depth++] = rope->right;
            rope = rope->left;
        }
    }
}
static void st_trim(msb_state_t *s) {
    while (s->empty) {
        st_slab_t *b = s->empty;
        st_empty_remove(s, b);
        st_available_remove(s, b);
        if (b->prev)
            b->prev->next = b->next;
        else
            s->slabs = b->next;
        if (b->next)
            b->next->prev = b->prev;
        s->bytes -= sizeof(*b);
        cbm_free(CBM_MEM_CLASS_OTHER, b);
        msb_work(1);
    }
}
static void st_state_clear(msb_state_t *s) {
    st_rope_drop(s, s->item_a);
    st_rope_drop(s, s->item_b);
    s->item_a = s->item_b = s->capture_a = s->capture_b = NULL;
    for (int i = 0; i < s->files; i++)
        for (int mode = 0; mode < 2; mode++) {
            st_component_drop(s, s->components[(size_t)i * 2 + mode]);
            msb_work(1);
        }
    s->bytes -= (size_t)s->files * 2 * sizeof(*s->components);
    cbm_free(CBM_MEM_CLASS_OTHER, s->components);
    s->components = NULL;
    s->files = 0;
    st_trim(s);
    cbm_ht_free(s->symbols);
    s->symbols = NULL;
    cbm_arena_destroy(&s->names);
    s->next_symbol = 0;
}
static bool st_state_epoch(msb_state_t *s, const cbm_msb_t *m) {
    if (s->epoch == UINT64_MAX || (size_t)m->nfiles > SIZE_MAX / 2 / sizeof(*s->components))
        return false;
    s->epoch++;
    s->symbols = cbm_ht_create(CBM_SZ_64);
    cbm_arena_init_lazy(&s->names, CBM_ARENA_APPEND_BLOCK);
    s->components = (st_component_t **)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                                  (size_t)m->nfiles * 2 * sizeof(*s->components));
    if (!s->symbols || (!s->components && m->nfiles))
        return false;
    s->generation = m->generation;
    s->files = m->nfiles;
    s->bytes += (size_t)s->files * 2 * sizeof(*s->components);
    msb_work((size_t)s->files * 2 * sizeof(*s->components));
    return true;
}
static msb_state_t *st_state_new(const cbm_msb_t *m) {
    msb_state_t *s = (msb_state_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*s));
    if (!s)
        return NULL;
    s->bytes = sizeof(*s);
    msb_work(sizeof(*s));
    if (!st_state_epoch(s, m)) {
        st_state_free(s);
        return NULL;
    }
    return s;
}
static void st_state_free(msb_state_t *s) {
    if (!s)
        return;
    st_state_clear(s);
    cbm_free(CBM_MEM_CLASS_OTHER, s);
}
static st_component_t *st_run(msb_eval_t *ev, int file) {
    ev->completed = NULL;
    if (file >= 0 && !ev->oom)
        msb_pass1(ev, file);
    st_component_t *out = ev->completed;
    ev->completed = NULL;
    return out;
}
static bool st_context_eval(cbm_msb_eval_context_t *context, const char *project_rel,
                            cbm_msb_result_t *out) {
    memset(out, 0, sizeof(*out));
    const cbm_msb_t *m = context->m;
    msb_state_t *s = context->state;
    if (s->generation != m->generation) {
        context_drop_items(context);
        st_state_clear(s);
        context_clear_nearest(context);
        if (!st_state_epoch(s, m)) {
            st_state_clear(s);
            return false;
        }
    }
    int project = project_rel ? msb_file_index(m, project_rel) : CBM_NOT_FOUND;
    if (project < 0 || !s->symbols || !s->components)
        return false;
    msb_eval_t ev = {.m = m, .state = s, .main_project = project};
    msb_live_t live = {.owners = {&ev, &context->item_owner},
                       .metadata_bytes = &context->nearest_bytes,
                       .state_bytes = &s->bytes,
                       .state_names = &s->names};
    ev.live = &live;
    /* One call-local arena pair; components never acquire an arena pair. */
    cbm_arena_init(&ev.arena);
    cbm_arena_init(&ev.scratch);
    ev_peak(&ev);
    msb_seed(&ev, project);
    cbm_arena_reset(&ev.scratch);
    int props =
        !ev.oom ? context_nearest(context, &ev, m->files[project].dir, false) : CBM_NOT_FOUND;
    st_component_t *prefix = st_run(&ev, props);
    st_component_t *local = st_run(&ev, project);
    int targets =
        !ev.oom ? context_nearest(context, &ev, m->files[project].dir, true) : CBM_NOT_FOUND;
    st_component_t *target = st_run(&ev, targets);
    bool ok = false;
    if (!ev.oom) {
        st_rope_t *a = prefix ? prefix->items : NULL, *b = target ? target->items : NULL;
        if (s->item_a != a || s->item_b != b) {
            context_drop_items(context);
            st_rope_drop(s, s->item_a);
            st_rope_drop(s, s->item_b);
            s->item_a = st_rope_hold(a);
            s->item_b = st_rope_hold(b);
        }
        s->capture_a = a;
        s->capture_b = b;
        msb_ulist_t inc = {0}, rem = {0};
        const char *const *implicit = msb_implicit(&ev, project);
        for (int i = 0; !ev.oom && implicit && implicit[i]; i++)
            ulist_add(&ev, &inc, 'n', "", implicit[i]);
        ok = !ev.oom && apply_items(context, &ev, &inc, &rem);
        if (ok) {
            st_eval_rope(&ev, local ? local->items : NULL, &inc, &rem);
            ok = !ev.oom && msb_result(&ev, &inc, &rem, out);
        }
        s->capture_a = s->capture_b = NULL;
    }
    while (ev.builder) {
        st_component_t *c = ev.builder;
        ev.builder = c->parent;
        c->parent = NULL;
        st_component_drop(s, c);
    }
    st_component_drop(s, prefix);
    st_component_drop(s, local);
    st_component_drop(s, target);
    st_component_drop(s, ev.completed);
    st_view_drop(s, &ev.view);
    eval_clear(&ev);
    st_trim(s);
    if (!ok)
        cbm_msb_result_free(out);
    return ok;
}

void cbm_msb_result_free(cbm_msb_result_t *r) {
    if (!r) {
        return;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, r->mem);
    memset(r, 0, sizeof(*r));
}
