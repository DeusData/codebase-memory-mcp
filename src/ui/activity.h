#ifndef CBM_UI_ACTIVITY_H
#define CBM_UI_ACTIVITY_H

#include <stdbool.h>
#include <stdint.h>
#include <sqlite3/sqlite3.h>

/* Local daemon-owned activity journal, separate from replaceable graph indexes.
 * Callers serialize a connection; independent processes use SQLite WAL/timeout. */
sqlite3 *cbm_activity_open(const char *path);
bool cbm_activity_log(sqlite3 *db, const char *line);
/* Canonical frontend writer: 1 inserted/repaired, 0 duplicate, -1 unavailable.
 * Historical import never assigns a project to previously unknown evidence. */
int cbm_activity_ui_log(sqlite3 *db, const char *line, bool historical);
char *cbm_activity_ui_logs(sqlite3 *db, int limit, const char *path, const char *export_path,
                           const char *previous_path);
/* Returns HTTP status. Replies are heap allocated, caller frees them. */
int cbm_activity_ingest(sqlite3 *db, const char *body, char **reply);
char *cbm_activity_agents(sqlite3 *db, const char *project, int64_t after, int limit);
char *cbm_activity_logs(sqlite3 *db, int64_t after, int limit, int min_level);
/* NULL project includes all projects unless unattributed=true. Filters apply
 * before limits/cursors/counts; legacy rows have unknown (NULL) provenance. */
char *cbm_activity_logs_scoped(sqlite3 *db, int64_t after, int limit, int min_level,
                               const char *project, bool unattributed);

char *cbm_activity_logs_filtered(sqlite3 *db, int64_t after, int limit, int min_level,
                                 const char *project, bool unattributed, const char *query);

#endif
