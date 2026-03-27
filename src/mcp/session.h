/*
 * session.h — Ephemeral session memory for MCP server (Phase 7A).
 *
 * Tracks files read/edited, symbols queried, areas explored, and impact
 * analyses run during a single Claude Code session.  State lives in memory
 * only — not persisted to SQLite — and resets on MCP server restart.
 */
#ifndef CBM_SESSION_H
#define CBM_SESSION_H

#include <stdbool.h>
#include <time.h>

/* Opaque session state handle. */
typedef struct cbm_session_state cbm_session_state_t;

/* Iterator callback: receives (key, userdata) for each entry in a set. */
typedef void (*cbm_session_iter_fn)(const char *key, void *userdata);

/* ── Lifecycle ─────────────────────────────────────────────────── */

cbm_session_state_t *cbm_session_create(void);
void cbm_session_free(cbm_session_state_t *s);

/* ── Tracking ──────────────────────────────────────────────────── */

void cbm_session_track_file_read(cbm_session_state_t *s, const char *path);
void cbm_session_track_file_edited(cbm_session_state_t *s, const char *path);
void cbm_session_track_symbol(cbm_session_state_t *s, const char *name);
void cbm_session_track_area(cbm_session_state_t *s, const char *keyword);
void cbm_session_track_impact(cbm_session_state_t *s, const char *symbol);
void cbm_session_bump_query_count(cbm_session_state_t *s);

/* ── Counts ────────────────────────────────────────────────────── */

int cbm_session_query_count(const cbm_session_state_t *s);
int cbm_session_files_read_count(const cbm_session_state_t *s);
int cbm_session_files_edited_count(const cbm_session_state_t *s);
int cbm_session_symbols_count(const cbm_session_state_t *s);
int cbm_session_areas_count(const cbm_session_state_t *s);
int cbm_session_impacts_count(const cbm_session_state_t *s);
time_t cbm_session_start_time(const cbm_session_state_t *s);

/* ── Membership checks ─────────────────────────────────────────── */

bool cbm_session_has_file_read(const cbm_session_state_t *s, const char *path);
bool cbm_session_has_file_edited(const cbm_session_state_t *s, const char *path);
bool cbm_session_has_symbol(const cbm_session_state_t *s, const char *name);
bool cbm_session_has_area(const cbm_session_state_t *s, const char *keyword);

/* ── Iteration ─────────────────────────────────────────────────── */

void cbm_session_foreach_file_read(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud);
void cbm_session_foreach_file_edited(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud);
void cbm_session_foreach_symbol(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud);
void cbm_session_foreach_area(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud);
void cbm_session_foreach_impact(const cbm_session_state_t *s, cbm_session_iter_fn fn, void *ud);

#endif /* CBM_SESSION_H */
