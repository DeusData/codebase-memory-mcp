/*
 * session.c — Ephemeral session memory implementation (Phase 7A).
 *
 * Uses CBMHashTable as sets (value = sentinel, key = strdup'd string).
 * Keys are owned by the session and freed on session_free().
 */
#include "mcp/session.h"
#include "foundation/hash_table.h"

#include <stdlib.h>
#include <string.h>

#define SESSION_SET_INITIAL_CAP 32
#define SET_SENTINEL ((void *)1)

struct cbm_session_state {
    CBMHashTable *files_read;
    CBMHashTable *files_edited;
    CBMHashTable *symbols_queried;
    CBMHashTable *areas_explored;
    CBMHashTable *impact_analyses;
    int query_count;
    time_t start_time;
};

/* ── Internal helpers ──────────────────────────────────────────── */

/* Callback for cbm_ht_foreach: free the strdup'd key. */
static void free_key_cb(const char *key, void *value, void *userdata) {
    (void)value;
    (void)userdata;
    free((void *)key);
}

/* Insert a key into a hash-table set if not already present. */
static void set_add(CBMHashTable *ht, const char *key) {
    if (!ht || !key) {
        return;
    }
    if (cbm_ht_has(ht, key)) {
        return;
    }
    char *owned = strdup(key);
    if (owned) {
        cbm_ht_set(ht, owned, SET_SENTINEL);
    }
}

/* Free all owned keys then free the table itself. */
static void set_free(CBMHashTable *ht) {
    if (!ht) {
        return;
    }
    cbm_ht_foreach(ht, free_key_cb, NULL);
    cbm_ht_free(ht);
}

/* Adapter: wraps cbm_session_iter_fn for cbm_ht_foreach. */
typedef struct {
    cbm_session_iter_fn fn;
    void *userdata;
} iter_adapter_t;

static void iter_adapter_cb(const char *key, void *value, void *userdata) {
    (void)value;
    iter_adapter_t *a = (iter_adapter_t *)userdata;
    a->fn(key, a->userdata);
}

static void set_foreach(const CBMHashTable *ht, cbm_session_iter_fn fn, void *ud) {
    if (!ht || !fn) {
        return;
    }
    iter_adapter_t adapter = {.fn = fn, .userdata = ud};
    cbm_ht_foreach(ht, iter_adapter_cb, &adapter);
}

/* ── Lifecycle ─────────────────────────────────────────────────── */

cbm_session_state_t *cbm_session_create(void) {
    cbm_session_state_t *s = calloc(1, sizeof(*s));
    if (!s) {
        return NULL;
    }
    s->files_read = cbm_ht_create(SESSION_SET_INITIAL_CAP);
    s->files_edited = cbm_ht_create(SESSION_SET_INITIAL_CAP);
    s->symbols_queried = cbm_ht_create(SESSION_SET_INITIAL_CAP);
    s->areas_explored = cbm_ht_create(SESSION_SET_INITIAL_CAP);
    s->impact_analyses = cbm_ht_create(SESSION_SET_INITIAL_CAP);
    if (!s->files_read || !s->files_edited || !s->symbols_queried || !s->areas_explored ||
        !s->impact_analyses) {
        cbm_session_free(s);
        return NULL;
    }
    s->start_time = time(NULL);
    return s;
}

void cbm_session_free(cbm_session_state_t *s) {
    if (!s) {
        return;
    }
    set_free(s->files_read);
    set_free(s->files_edited);
    set_free(s->symbols_queried);
    set_free(s->areas_explored);
    set_free(s->impact_analyses);
    free(s);
}

/* ── Tracking ──────────────────────────────────────────────────── */

void cbm_session_track_file_read(cbm_session_state_t *s, const char *path) {
    if (s) {
        set_add(s->files_read, path);
    }
}

void cbm_session_track_file_edited(cbm_session_state_t *s, const char *path) {
    if (s) {
        set_add(s->files_edited, path);
    }
}

void cbm_session_track_symbol(cbm_session_state_t *s, const char *name) {
    if (s) {
        set_add(s->symbols_queried, name);
    }
}

void cbm_session_track_area(cbm_session_state_t *s, const char *keyword) {
    if (s) {
        set_add(s->areas_explored, keyword);
    }
}

void cbm_session_track_impact(cbm_session_state_t *s, const char *symbol) {
    if (s) {
        set_add(s->impact_analyses, symbol);
    }
}

void cbm_session_bump_query_count(cbm_session_state_t *s) {
    if (s) {
        s->query_count++;
    }
}

/* ── Counts ────────────────────────────────────────────────────── */

int cbm_session_query_count(const cbm_session_state_t *s) {
    return s ? s->query_count : 0;
}

int cbm_session_files_read_count(const cbm_session_state_t *s) {
    return s ? (int)cbm_ht_count(s->files_read) : 0;
}

int cbm_session_files_edited_count(const cbm_session_state_t *s) {
    return s ? (int)cbm_ht_count(s->files_edited) : 0;
}

int cbm_session_symbols_count(const cbm_session_state_t *s) {
    return s ? (int)cbm_ht_count(s->symbols_queried) : 0;
}

int cbm_session_areas_count(const cbm_session_state_t *s) {
    return s ? (int)cbm_ht_count(s->areas_explored) : 0;
}

int cbm_session_impacts_count(const cbm_session_state_t *s) {
    return s ? (int)cbm_ht_count(s->impact_analyses) : 0;
}

time_t cbm_session_start_time(const cbm_session_state_t *s) {
    return s ? s->start_time : 0;
}

/* ── Membership checks ─────────────────────────────────────────── */

bool cbm_session_has_file_read(const cbm_session_state_t *s, const char *path) {
    return s && path && cbm_ht_has(s->files_read, path);
}

bool cbm_session_has_file_edited(const cbm_session_state_t *s, const char *path) {
    return s && path && cbm_ht_has(s->files_edited, path);
}

bool cbm_session_has_area(const cbm_session_state_t *s, const char *keyword) {
    return s && keyword && cbm_ht_has(s->areas_explored, keyword);
}

bool cbm_session_has_symbol(const cbm_session_state_t *s, const char *name) {
    return s && name && cbm_ht_has(s->symbols_queried, name);
}

/* ── Iteration ─────────────────────────────────────────────────── */

void cbm_session_foreach_file_read(const cbm_session_state_t *s, cbm_session_iter_fn fn,
                                   void *ud) {
    if (s) {
        set_foreach(s->files_read, fn, ud);
    }
}

void cbm_session_foreach_file_edited(const cbm_session_state_t *s, cbm_session_iter_fn fn,
                                     void *ud) {
    if (s) {
        set_foreach(s->files_edited, fn, ud);
    }
}

void cbm_session_foreach_symbol(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud) {
    if (s) {
        set_foreach(s->symbols_queried, fn, ud);
    }
}

void cbm_session_foreach_area(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud) {
    if (s) {
        set_foreach(s->areas_explored, fn, ud);
    }
}

void cbm_session_foreach_impact(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud) {
    if (s) {
        set_foreach(s->impact_analyses, fn, ud);
    }
}
