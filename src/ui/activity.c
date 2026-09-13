#include "ui/activity.h"
#include <yyjson/yyjson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef _WIN32
#include <sys/stat.h>
#endif

#define AGENT_CAP 10000
#define LOG_NORMAL_CAP 3000
#define LOG_ERROR_CAP 2000
#define EVENT_MAX 16384

sqlite3 *cbm_activity_open(const char *path) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(path, &db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                        NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
#ifndef _WIN32
    if (strcmp(path, ":memory:"))
        (void)chmod(path, 0600);
#endif
    sqlite3_busy_timeout(db, 1000);
    sqlite3_stmt *version_stmt = NULL;
    int version = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA user_version", -1, &version_stmt, NULL) == SQLITE_OK &&
        sqlite3_step(version_stmt) == SQLITE_ROW)
        version = sqlite3_column_int(version_stmt, 0);
    sqlite3_finalize(version_stmt);
    if (version > 4) {
        sqlite3_close(db);
        return NULL;
    }
    /* An additive, versioned migration; graph schema/rebuilds never erase logs. */
    const char *schema =
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; BEGIN IMMEDIATE;"
        "CREATE TABLE IF NOT EXISTS activity_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
        "INSERT OR IGNORE INTO activity_meta VALUES('generation',lower(hex(randomblob(16))));"
        "CREATE TABLE IF NOT EXISTS agent_events("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,project TEXT NOT NULL,run TEXT NOT NULL,"
        "seq INTEGER NOT NULL,payload TEXT NOT NULL,UNIQUE(project,run,seq));"
        "CREATE INDEX IF NOT EXISTS agent_events_project ON agent_events(project,id);"
        "CREATE TABLE IF NOT EXISTS agent_retention(project TEXT PRIMARY KEY,pruned_through "
        "INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS agent_retired(project TEXT NOT NULL,run TEXT NOT NULL,max_seq "
        "INTEGER NOT NULL,PRIMARY KEY(project,run));"
        "CREATE TABLE IF NOT EXISTS ui_log_retired(session TEXT PRIMARY KEY,max_seq INTEGER NOT "
        "NULL);"
        "CREATE TABLE IF NOT EXISTS daemon_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "ts TEXT NOT NULL,level INTEGER NOT NULL,source TEXT NOT NULL,message TEXT NOT NULL);"
        "CREATE INDEX IF NOT EXISTS daemon_logs_level ON daemon_logs(level,id);";
    if (sqlite3_exec(db, schema, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    sqlite3_stmt *columns = NULL;
    bool has_event_key = false, has_project = false, has_origin = false;
    if (sqlite3_prepare_v2(db, "PRAGMA table_info(daemon_logs)", -1, &columns, NULL) == SQLITE_OK) {
        while (sqlite3_step(columns) == SQLITE_ROW) {
            const char *column = (const char *)sqlite3_column_text(columns, 1);
            has_event_key |= !strcmp(column, "event_key");
            has_project |= !strcmp(column, "project");
            has_origin |= !strcmp(column, "origin");
        }
    }
    sqlite3_finalize(columns);
    if ((!has_event_key && sqlite3_exec(db, "ALTER TABLE daemon_logs ADD COLUMN event_key TEXT",
                                        NULL, NULL, NULL) != SQLITE_OK) ||
        (!has_project && sqlite3_exec(db, "ALTER TABLE daemon_logs ADD COLUMN project TEXT", NULL,
                                      NULL, NULL) != SQLITE_OK) ||
        (!has_origin &&
         sqlite3_exec(
             db,
             "ALTER TABLE daemon_logs ADD COLUMN origin TEXT NOT NULL DEFAULT 'daemon';"
             "UPDATE daemon_logs SET origin='frontend' WHERE CASE WHEN json_valid(message) THEN "
             "json_type(message,'$.received')='text' AND json_type(message,'$.page')='text' AND "
             "json_type(message,'$.session')='text' ELSE 0 END",
             NULL, NULL, NULL) != SQLITE_OK) ||
        sqlite3_exec(db,
                     "CREATE INDEX IF NOT EXISTS daemon_logs_origin ON daemon_logs(origin,id);"
                     "CREATE UNIQUE INDEX IF NOT EXISTS daemon_logs_event_key ON "
                     "daemon_logs(event_key); CREATE INDEX IF NOT EXISTS daemon_logs_project ON "
                     "daemon_logs(project,level,id); PRAGMA user_version=4; COMMIT",
                     NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    return db;
}

static const char *string_field(yyjson_val *obj, const char *key) {
    const char *s = yyjson_get_str(yyjson_obj_get(obj, key));
    return s ? s : "";
}

/* Only explicit top-level provenance from a producer. Never infer a project
 * from a page URL, path, nested worker record or message text. Text-format
 * cbm_log atoms cannot contain spaces; canonical index project names are safe
 * atoms. Existing journal rows remain NULL during migration. */
static const char *log_project(yyjson_val *root, const char *line, char out[256]) {
    if (root) {
        const char *value = string_field(root, "project");
        return *value && strlen(value) < 256 ? value : NULL;
    }
    if (strncmp(line, "level=", strlen("level=")))
        return NULL;
    const char *atom = line;
    while (*atom) {
        size_t length = strcspn(atom, " \r\n\t");
        if (length > strlen("project=") && !strncmp(atom, "project=", strlen("project="))) {
            size_t size = length - strlen("project=");
            if (size >= 256)
                return NULL;
            memcpy(out, atom + strlen("project="), size);
            out[size] = '\0';
            return out;
        }
        atom += length;
        atom += strspn(atom, " \r\n\t");
    }
    return NULL;
}

/* Expiring payloads also retires their frontend sequence identity. Delayed
 * batch retries cannot resurrect an old warning after routine/error retention. */
static bool prune_logs(sqlite3 *db, int level) {
    sqlite3_stmt *st = NULL;
    const char *boundary =
        level >= 2
            ? "SELECT id FROM daemon_logs WHERE level>=2 ORDER BY id DESC LIMIT 1 OFFSET 2000"
            : "SELECT id FROM daemon_logs WHERE level<2 ORDER BY id DESC LIMIT 1 OFFSET 3000";
    if (sqlite3_prepare_v2(db, boundary, -1, &st, NULL) != SQLITE_OK)
        return false;
    int rc = sqlite3_step(st);
    int64_t cutoff = rc == SQLITE_ROW ? sqlite3_column_int64(st, 0) : 0;
    sqlite3_finalize(st);
    if (rc == SQLITE_DONE)
        return true;
    if (rc != SQLITE_ROW || sqlite3_exec(db, "SAVEPOINT prune_logs", NULL, NULL, NULL) != SQLITE_OK)
        return false;
    bool ok =
        sqlite3_prepare_v2(
            db,
            "INSERT INTO ui_log_retired(session,max_seq) "
            "SELECT json_extract(message,'$.session'),max(json_extract(message,'$.seq')) "
            "FROM daemon_logs WHERE id<=?1 AND (level>=2)=?2 AND origin='frontend' AND "
            "CASE WHEN json_valid(message) THEN json_type(message,'$.session')='text' AND "
            "length(json_extract(message,'$.session'))>0 AND json_type(message,'$.seq')='integer' "
            "ELSE 0 END GROUP BY json_extract(message,'$.session') "
            "ON CONFLICT(session) DO UPDATE SET max_seq=max(max_seq,excluded.max_seq)",
            -1, &st, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_int64(st, 1, cutoff);
        sqlite3_bind_int(st, 2, level >= 2);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }
    sqlite3_finalize(st);
    st = NULL;
    if (ok)
        ok = sqlite3_prepare_v2(db, "DELETE FROM daemon_logs WHERE id<=?1 AND (level>=2)=?2", -1,
                                &st, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_int64(st, 1, cutoff);
        sqlite3_bind_int(st, 2, level >= 2);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }
    sqlite3_finalize(st);
    if (ok)
        ok = sqlite3_exec(db, "RELEASE prune_logs", NULL, NULL, NULL) == SQLITE_OK;
    if (!ok)
        sqlite3_exec(db, "ROLLBACK TO prune_logs; RELEASE prune_logs", NULL, NULL, NULL);
    return ok;
}

/* Returns inserted / duplicate / failure. Frontend records are already bounded,
 * sanitized JSON; retain their full shape so both APIs share source evidence. */
static int activity_log(sqlite3 *db, const char *line, bool frontend, bool historical) {
    if (!db || !line)
        return -1;
    int level = 1;
    char source[256] = "daemon";
    yyjson_doc *doc = yyjson_read(line, strlen(line), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    if (frontend && (!yyjson_is_obj(root) || strlen(line) > 131072)) {
        yyjson_doc_free(doc);
        return -1;
    }
    const char *name = string_field(root, "level");
    if (*name) {
        if (!strcmp(name, "error"))
            level = 3;
        else if (!strcmp(name, "warn"))
            level = 2;
        else if (!strcmp(name, "debug"))
            level = 0;
    } else if (!strncmp(line, "level=error ", 12) || !strncmp(line, "ui.error ", 9))
        level = 3;
    else if (!strncmp(line, "level=warn ", 11) || !strncmp(line, "ui.warn ", 8))
        level = 2;
    else if (!strncmp(line, "level=debug ", 12))
        level = 0;
    const char *tag = string_field(root, "msg");
    if (!*tag)
        tag = string_field(root, "source");
    if (*tag)
        snprintf(source, sizeof(source), "%s", tag);
    else {
        tag = strstr(line, "msg=");
        if (tag) {
            tag += 4;
            size_t len = strcspn(tag, " \r\n");
            snprintf(source, sizeof(source), "%.*s", (int)(len < 255 ? len : 255), tag);
        } else if (strstr(line, "[ui/"))
            snprintf(source, sizeof(source), "frontend");
    }
    sqlite3_stmt *st = NULL;
    bool ok = sqlite3_prepare_v2(
                  db,
                  "INSERT INTO daemon_logs(ts,level,source,message,event_key,project,origin) "
                  "SELECT coalesce(?6,strftime('%Y-%m-%dT%H:%M:%fZ','now')),?1,?2,?3,?4,?5,?7 "
                  "WHERE ?8 IS NULL OR NOT EXISTS(SELECT 1 FROM ui_log_retired WHERE session=?8 "
                  "AND max_seq>=?9) "
                  "ON CONFLICT(event_key) DO UPDATE SET message=excluded.message,origin='frontend' "
                  "WHERE daemon_logs.origin='daemon' AND excluded.origin='frontend'",
                  -1, &st, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_int(st, 1, level);
        sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, line,
                          (int)(!frontend && strlen(line) > 8192 ? 8192 : strlen(line)),
                          SQLITE_TRANSIENT);
        const char *session = string_field(root, "session");
        yyjson_val *seq = yyjson_obj_get(root, "seq");
        if (*session && yyjson_is_int(seq)) {
            char key[512];
            snprintf(key, sizeof(key), "%zu:%s:%lld", strlen(session), session,
                     (long long)yyjson_get_sint(seq));
            sqlite3_bind_text(st, 4, key, -1, SQLITE_TRANSIENT);
        } else
            sqlite3_bind_null(st, 4);
        char project_atom[256];
        const char *project = log_project(root, line, project_atom);
        if (project && !historical)
            sqlite3_bind_text(st, 5, project, -1, SQLITE_TRANSIENT);
        else
            sqlite3_bind_null(st, 5);
        const char *received = frontend ? string_field(root, "received") : "";
        if (*received)
            sqlite3_bind_text(st, 6, received, -1, SQLITE_TRANSIENT);
        else
            sqlite3_bind_null(st, 6);
        sqlite3_bind_text(st, 7, frontend ? "frontend" : "daemon", -1, SQLITE_STATIC);
        if (frontend && *session && yyjson_is_int(seq)) {
            sqlite3_bind_text(st, 8, session, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 9, yyjson_get_sint(seq));
        } else {
            sqlite3_bind_null(st, 8);
            sqlite3_bind_null(st, 9);
        }
        ok = sqlite3_step(st) == SQLITE_DONE;
    }
    int inserted = ok ? sqlite3_changes(db) : 0;
    sqlite3_finalize(st);
    yyjson_doc_free(doc);
    /* Reserve error/warning capacity independently of routine request traffic. */
    if (ok && inserted)
        ok = prune_logs(db, level);
    return ok ? inserted : -1;
}

bool cbm_activity_log(sqlite3 *db, const char *line) {
    return activity_log(db, line, false, false) >= 0;
}

int cbm_activity_ui_log(sqlite3 *db, const char *line, bool historical) {
    return activity_log(db, line, true, historical);
}

static char *write_reply(yyjson_mut_doc *doc) {
    char *reply = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return reply;
}

/* Compatibility endpoint: original JSON lines, now from the authoritative
 * journal. Bound both rows and bytes; export-file rotation cannot change them. */
char *cbm_activity_ui_logs(sqlite3 *db, int limit, const char *path, const char *export_path,
                           const char *previous_path) {
    if (!db)
        return NULL;
    if (limit <= 0 || limit > 1000)
        limit = 200;
    sqlite3_stmt *st = NULL;
    if (sqlite3_exec(db, "SAVEPOINT ui_tail", NULL, NULL, NULL) != SQLITE_OK)
        return NULL;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc), *lines = yyjson_mut_arr(doc);
    yyjson_mut_doc_set_root(doc, root);
    int rc = sqlite3_prepare_v2(
        db,
        "SELECT COUNT(*),coalesce(SUM(length(CAST(message AS BLOB))),0) FROM daemon_logs "
        "WHERE origin='frontend'",
        -1, &st, NULL);
    if (rc != SQLITE_OK || sqlite3_step(st) != SQLITE_ROW)
        goto fail;
    int64_t total = sqlite3_column_int64(st, 0), bytes = sqlite3_column_int64(st, 1);
    sqlite3_finalize(st);
    st = NULL;
    bool historical_partial = false;
    rc = sqlite3_prepare_v2(db, "SELECT value FROM activity_meta WHERE key='ui_legacy_import'", -1,
                            &st, NULL);
    if (rc != SQLITE_OK)
        goto fail;
    rc = sqlite3_step(st);
    if (rc == SQLITE_ROW)
        historical_partial = !strcmp((const char *)sqlite3_column_text(st, 0), "bounded");
    else if (rc != SQLITE_DONE)
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    rc = sqlite3_prepare_v2(
        db, "SELECT message FROM daemon_logs WHERE origin='frontend' ORDER BY id DESC LIMIT ?1", -1,
        &st, NULL);
    if (rc != SQLITE_OK)
        goto fail;
    sqlite3_bind_int(st, 1, limit);
    yyjson_mut_val *rows[1000];
    int count = 0;
    size_t emitted_bytes = 0;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        const char *message = (const char *)sqlite3_column_text(st, 0);
        size_t length = (size_t)sqlite3_column_bytes(st, 0);
        if (emitted_bytes + length > 256 * 1024)
            break;
        while (length && (message[length - 1] == '\n' || message[length - 1] == '\r'))
            length--;
        rows[count++] = yyjson_mut_strncpy(doc, message, length);
        emitted_bytes += length;
    }
    if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        goto fail;
    for (int i = count - 1; i >= 0; i--)
        yyjson_mut_arr_append(lines, rows[i]);
    sqlite3_finalize(st);
    st = NULL;
    if (sqlite3_exec(db, "RELEASE ui_tail", NULL, NULL, NULL) != SQLITE_OK)
        goto fail;
    yyjson_mut_obj_add_strcpy(doc, root, "path", path);
    yyjson_mut_obj_add_strcpy(doc, root, "export_path", export_path);
    if (previous_path && *previous_path)
        yyjson_mut_obj_add_strcpy(doc, root, "previous_path", previous_path);
    yyjson_mut_obj_add_str(doc, root, "source", "sqlite");
    yyjson_mut_obj_add_bool(doc, root, "persistent", true);
    yyjson_mut_obj_add_int(doc, root, "size_bytes", bytes);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_bool(doc, root, "partial", historical_partial || total > count);
    yyjson_mut_obj_add_bool(doc, root, "legacy_import_partial", historical_partial);
    yyjson_mut_obj_add_int(doc, root, "routine_retention", LOG_NORMAL_CAP);
    yyjson_mut_obj_add_int(doc, root, "priority_retention", LOG_ERROR_CAP);
    yyjson_mut_obj_add_val(doc, root, "lines", lines);
    return write_reply(doc);
fail:
    sqlite3_finalize(st);
    sqlite3_exec(db, "ROLLBACK TO ui_tail; RELEASE ui_tail", NULL, NULL, NULL);
    yyjson_mut_doc_free(doc);
    return NULL;
}

int cbm_activity_ingest(sqlite3 *db, const char *body, char **reply) {
    *reply = NULL;
    if (!db)
        return 503;
    yyjson_doc *doc = body ? yyjson_read(body, strlen(body), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *events = yyjson_obj_get(root, "events");
    const char *project = string_field(root, "project");
    if (!yyjson_is_arr(events) || yyjson_arr_size(events) > 100 || strlen(project) > 255) {
        yyjson_doc_free(doc);
        return 400;
    }
    size_t i, max;
    yyjson_val *e;
    yyjson_arr_foreach(events, i, max, e) {
        const char *run = string_field(e, "run");
        const char *agent = string_field(e, "agent");
        const char *tool = string_field(e, "tool");
        const char *phase = string_field(e, "phase");
        yyjson_val *seq = yyjson_obj_get(e, "seq");
        yyjson_val *ts = yyjson_obj_get(e, "ts");
        if (!*run || strlen(run) > 255 || !*agent || strlen(agent) > 255 || !*tool ||
            strlen(tool) > 255 || (!yyjson_is_uint(seq) && !yyjson_is_sint(seq)) ||
            yyjson_get_sint(seq) < 0 || yyjson_get_sint(seq) > 9007199254740991LL ||
            !yyjson_is_num(ts) || yyjson_get_num(ts) < 0 ||
            yyjson_get_num(ts) > 8640000000000000.0 ||
            (strcmp(phase, "start") && strcmp(phase, "end"))) {
            yyjson_doc_free(doc);
            return 400;
        }
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        yyjson_doc_free(doc);
        return 503;
    }
    sqlite3_stmt *st = NULL;
    bool ok = sqlite3_prepare_v2(
                  db,
                  "INSERT OR IGNORE INTO agent_events(project,run,seq,payload) SELECT ?1,?2,?3,?4 "
                  "WHERE NOT EXISTS(SELECT 1 FROM agent_retired WHERE project=?1 AND run=?2 AND "
                  "max_seq>=?3)",
                  -1, &st, NULL) == SQLITE_OK;
    int accepted = 0;
    yyjson_arr_foreach(events, i, max, e) {
        if (!ok)
            break;
        /* Persist only the event contract, never arbitrary tool responses. */
        yyjson_mut_doc *clean = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *obj = yyjson_mut_obj(clean);
        yyjson_mut_doc_set_root(clean, obj);
        const char *fields[] = {"ts",     "agent",  "run",        "seq",    "phase",
                                "tool",   "path",   "lines",      "detail", "intent",
                                "source", "replay", "ts_recorded"};
        for (size_t k = 0; k < sizeof(fields) / sizeof(fields[0]); k++) {
            yyjson_val *v = yyjson_obj_get(e, fields[k]);
            if (v)
                yyjson_mut_obj_add(obj, yyjson_mut_str(clean, fields[k]),
                                   yyjson_val_mut_copy(clean, v));
        }
        char *payload = write_reply(clean);
        if (!payload || strlen(payload) > EVENT_MAX) {
            free(payload);
            ok = false;
            break;
        }
        sqlite3_reset(st);
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, string_field(e, "run"), -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 3, yyjson_get_sint(yyjson_obj_get(e, "seq")));
        sqlite3_bind_text(st, 4, payload, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(st) == SQLITE_DONE;
        if (ok)
            accepted += sqlite3_changes(db);
        free(payload);
    }
    sqlite3_finalize(st);
    if (ok)
        ok = sqlite3_exec(
                 db,
                 "INSERT INTO agent_retired SELECT project,run,max(seq) FROM agent_events "
                 "WHERE id <= (SELECT id FROM agent_events ORDER BY id DESC LIMIT 1 OFFSET 10000) "
                 "GROUP BY project,run "
                 "ON CONFLICT(project,run) DO UPDATE SET max_seq=max(max_seq,excluded.max_seq);"
                 "INSERT INTO agent_retention SELECT project,max(id) FROM agent_events "
                 "WHERE id <= (SELECT id FROM agent_events ORDER BY id DESC LIMIT 1 "
                 "OFFSET 10000) GROUP BY project "
                 "ON CONFLICT(project) DO UPDATE SET "
                 "pruned_through=max(pruned_through,excluded.pruned_through);"
                 "DELETE FROM agent_events WHERE id <= (SELECT id FROM agent_events ORDER "
                 "BY id DESC LIMIT 1 OFFSET 10000); COMMIT",
                 NULL, NULL, NULL) == SQLITE_OK;
    if (!ok)
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    yyjson_doc_free(doc);
    if (!ok)
        return 503;
    yyjson_mut_doc *out = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *obj = yyjson_mut_obj(out);
    yyjson_mut_doc_set_root(out, obj);
    yyjson_mut_obj_add_int(out, obj, "accepted", accepted);
    yyjson_mut_obj_add_int(out, obj, "duplicates", (int)max - accepted);
    *reply = write_reply(out);
    return 200;
}

static bool generation(sqlite3 *db, yyjson_mut_doc *doc, yyjson_mut_val *obj) {
    sqlite3_stmt *st = NULL;
    bool ok = sqlite3_prepare_v2(db, "SELECT value FROM activity_meta WHERE key='generation'", -1,
                                 &st, NULL) == SQLITE_OK &&
              sqlite3_step(st) == SQLITE_ROW;
    if (ok)
        yyjson_mut_obj_add_strcpy(doc, obj, "generation", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return ok;
}

char *cbm_activity_agents(sqlite3 *db, const char *project, int64_t after, int limit) {
    if (!db)
        return NULL;
    if (limit < 1 || limit > 500)
        limit = 200;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *obj = yyjson_mut_obj(doc), *rows = yyjson_mut_arr(doc);
    yyjson_mut_doc_set_root(doc, obj);
    yyjson_mut_obj_add_val(doc, obj, "events", rows);
    if (!generation(db, doc, obj)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    sqlite3_stmt *st = NULL;
    int64_t oldest = 0, newest = 0, count = 0;
    if (sqlite3_prepare_v2(db,
                           "SELECT coalesce(min(id),0),coalesce(max(id),0),count(*) FROM "
                           "agent_events WHERE project=?1",
                           -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        oldest = sqlite3_column_int64(st, 0);
        newest = sqlite3_column_int64(st, 1);
        count = sqlite3_column_int64(st, 2);
    } else
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    int64_t pruned = 0;
    if (sqlite3_prepare_v2(db, "SELECT pruned_through FROM agent_retention WHERE project=?1", -1,
                           &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    int prune_status = sqlite3_step(st);
    if (prune_status == SQLITE_ROW)
        pruned = sqlite3_column_int64(st, 0);
    else if (prune_status != SQLITE_DONE)
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    bool reset = after > newest && newest > 0;
    if (reset)
        after = 0;
    if (sqlite3_prepare_v2(
            db,
            "SELECT id,payload FROM agent_events WHERE project=?1 AND id>?2 ORDER BY id LIMIT ?3",
            -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 2, after);
    sqlite3_bind_int(st, 3, limit);
    int64_t cursor = after;
    int row_status;
    while ((row_status = sqlite3_step(st)) == SQLITE_ROW) {
        cursor = sqlite3_column_int64(st, 0);
        const char *payload = (const char *)sqlite3_column_text(st, 1);
        yyjson_doc *row = yyjson_read(payload, strlen(payload), 0);
        if (row)
            yyjson_mut_arr_add_val(rows, yyjson_val_mut_copy(doc, yyjson_doc_get_root(row)));
        yyjson_doc_free(row);
    }
    if (row_status != SQLITE_DONE)
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    yyjson_mut_obj_add_int(doc, obj, "cursor", cursor);
    yyjson_mut_obj_add_int(doc, obj, "oldest_cursor", oldest);
    yyjson_mut_obj_add_int(doc, obj, "retained", count);
    yyjson_mut_obj_add_int(doc, obj, "retention_limit", AGENT_CAP);
    yyjson_mut_obj_add_bool(doc, obj, "has_more", cursor < newest);
    yyjson_mut_obj_add_bool(doc, obj, "reset", reset);
    yyjson_mut_obj_add_bool(doc, obj, "truncated", pruned > after);
    return write_reply(doc);
fail:
    sqlite3_finalize(st);
    yyjson_mut_doc_free(doc);
    return NULL;
}

char *cbm_activity_logs(sqlite3 *db, int64_t after, int limit, int min_level) {
    return cbm_activity_logs_scoped(db, after, limit, min_level, NULL, false);
}

char *cbm_activity_logs_scoped(sqlite3 *db, int64_t after, int limit, int min_level,
                               const char *project, bool unattributed) {
    return cbm_activity_logs_filtered(db, after, limit, min_level, project, unattributed, "");
}

char *cbm_activity_logs_filtered(sqlite3 *db, int64_t after, int limit, int min_level,
                                 const char *project, bool unattributed, const char *query) {
    if (!db)
        return NULL;
    if (limit < 1 || limit > 500)
        limit = 100;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *obj = yyjson_mut_obj(doc), *records = yyjson_mut_arr(doc),
                   *lines = yyjson_mut_arr(doc);
    yyjson_mut_doc_set_root(doc, obj);
    if (!generation(db, doc, obj)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    yyjson_mut_obj_add_val(doc, obj, "records", records);
    yyjson_mut_obj_add_val(doc, obj, "lines", lines);
    sqlite3_stmt *st = NULL;
    yyjson_mut_obj_add_str(doc, obj, "scope",
                           project        ? "project"
                           : unattributed ? "unattributed"
                                          : "daemon");
    if (project)
        yyjson_mut_obj_add_strcpy(doc, obj, "project", project);
    else
        yyjson_mut_obj_add_null(doc, obj, "project");
    yyjson_mut_obj_add_strcpy(doc, obj, "query", query ? query : "");
    const char *sql =
        after >= 0 ? "SELECT id,ts,level,source,message,project FROM daemon_logs WHERE id>?1 AND "
                     "level>=?2 AND (?4 IS NULL OR project=?4) AND (?5=0 OR project IS NULL) "
                     "AND instr(lower(ts||' '||source||' '||message||' "
                     "'||coalesce(project,'')),lower(?6))>0 "
                     "ORDER BY id LIMIT ?3"
                   : "SELECT * FROM (SELECT id,ts,level,source,message,project FROM daemon_logs "
                     "WHERE level>=?2 AND (?4 IS NULL OR project=?4) AND (?5=0 OR project IS NULL) "
                     "AND instr(lower(ts||' '||source||' '||message||' "
                     "'||coalesce(project,'')),lower(?6))>0 "
                     "ORDER BY id DESC LIMIT ?3) ORDER BY id";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    if (after >= 0)
        sqlite3_bind_int64(st, 1, after);
    sqlite3_bind_int(st, 2, min_level);
    sqlite3_bind_int(st, 3, limit);
    sqlite3_bind_text(st, 4, project, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 5, unattributed);
    sqlite3_bind_text(st, 6, query ? query : "", -1, SQLITE_STATIC);
    int64_t cursor = after > 0 ? after : 0;
    const char *levels[] = {"debug", "info", "warn", "error"};
    int row_status;
    while ((row_status = sqlite3_step(st)) == SQLITE_ROW) {
        cursor = sqlite3_column_int64(st, 0);
        if (sqlite3_column_int(st, 2) < 0 || sqlite3_column_int(st, 2) > 3)
            goto fail;
        yyjson_mut_val *row = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, row, "id", cursor);
        yyjson_mut_obj_add_strcpy(doc, row, "ts", (const char *)sqlite3_column_text(st, 1));
        yyjson_mut_obj_add_str(doc, row, "level", levels[sqlite3_column_int(st, 2)]);
        yyjson_mut_obj_add_strcpy(doc, row, "source", (const char *)sqlite3_column_text(st, 3));
        yyjson_mut_obj_add_strcpy(doc, row, "message", (const char *)sqlite3_column_text(st, 4));
        if (sqlite3_column_type(st, 5) == SQLITE_NULL)
            yyjson_mut_obj_add_null(doc, row, "project");
        else
            yyjson_mut_obj_add_strcpy(doc, row, "project",
                                      (const char *)sqlite3_column_text(st, 5));
        const char *message = (const char *)sqlite3_column_text(st, 4);
        yyjson_doc *frontend = yyjson_read(message, strlen(message), 0);
        yyjson_val *payload = frontend ? yyjson_doc_get_root(frontend) : NULL;
        if (*string_field(payload, "session")) {
            char legacy[512];
            snprintf(legacy, sizeof(legacy), "ui.%s %.48s: %.400s",
                     levels[sqlite3_column_int(st, 2)], string_field(payload, "source"),
                     string_field(payload, "message"));
            yyjson_mut_arr_add_strcpy(doc, lines, legacy);
        } else
            yyjson_mut_arr_add_strcpy(doc, lines, message);
        yyjson_doc_free(frontend);
        yyjson_mut_arr_add_val(records, row);
    }
    if (row_status != SQLITE_DONE)
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    yyjson_mut_obj_add_int(doc, obj, "cursor", cursor);
    yyjson_mut_obj_add_int(doc, obj, "retention_limit", LOG_NORMAL_CAP + LOG_ERROR_CAP);
    yyjson_mut_obj_add_bool(doc, obj, "persistent", true);
    if (sqlite3_prepare_v2(
            db,
            "SELECT count(*),coalesce(min(id),0),coalesce(max(id),0) FROM "
            "daemon_logs WHERE level>=?1 AND (?2 IS NULL OR project=?2) AND "
            "(?3=0 OR project IS NULL) AND "
            "instr(lower(ts||' '||source||' '||message||' '||coalesce(project,'')),lower(?4))>0",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, min_level);
        sqlite3_bind_text(st, 2, project, -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 3, unattributed);
        sqlite3_bind_text(st, 4, query ? query : "", -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            yyjson_mut_obj_add_int(doc, obj, "total", sqlite3_column_int64(st, 0));
            yyjson_mut_obj_add_int(doc, obj, "oldest_cursor", sqlite3_column_int64(st, 1));
            yyjson_mut_obj_add_bool(doc, obj, "has_more", cursor < sqlite3_column_int64(st, 2));
        } else
            goto fail;
    } else
        goto fail;
    sqlite3_finalize(st);
    return write_reply(doc);
fail:
    sqlite3_finalize(st);
    yyjson_mut_doc_free(doc);
    return NULL;
}
