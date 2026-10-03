/*
 * query_sequence.c — Call sequence extraction for sequence diagrams.
 */
#include "diagram/diagram.h"
#include "store/store.h"
#include "foundation/str_util.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    SEQ_MAX_VISITED = 512,
    SEQ_MAX_STEPS = 128,
};

typedef struct {
    sqlite3 *db;
    sqlite3_stmt *stmt_calls;
    cbm_seq_trace_t *trace;
    int max_depth;
    int max_participants;
    int nodes_analyzed;
    int edges_traversed;
    int64_t visited_stack[SEQ_MAX_VISITED];
    int visited_stack_depth;
} seq_query_ctx_t;

static int get_or_add_participant(seq_query_ctx_t *ctx, const char *file_path, const char *symbol_name) {
    const char *raw_file = (file_path && file_path[0] != '\0') ? file_path : symbol_name;
    const char *base = cbm_path_base(raw_file);
    if (!base || base[0] == '\0') {
        base = symbol_name ? symbol_name : "unknown";
    }

    /* Check existing participants */
    for (int i = 0; i < ctx->trace->participant_count; i++) {
        if (strcmp(ctx->trace->participants[i].file, base) == 0) {
            return i;
        }
    }

    /* If limit reached, map to the last participant or return current count - 1 */
    if (ctx->trace->participant_count >= ctx->max_participants) {
        return ctx->trace->participant_count - 1;
    }

    char id_buf[64];
    snprintf(id_buf, sizeof(id_buf), "P%d", ctx->trace->participant_count);
    return cbm_seq_trace_add_participant(ctx->trace, id_buf, base, base);
}

static bool is_in_stack(const seq_query_ctx_t *ctx, int64_t node_id) {
    for (int i = 0; i < ctx->visited_stack_depth; i++) {
        if (ctx->visited_stack[i] == node_id) {
            return true;
        }
    }
    return false;
}

static void traverse_calls(seq_query_ctx_t *ctx, int64_t current_id, const char *current_name,
                           const char *current_file, int current_participant_idx, int depth) {
    if (depth >= ctx->max_depth || ctx->trace->message_count >= SEQ_MAX_STEPS) {
        return;
    }
    if (ctx->visited_stack_depth >= SEQ_MAX_VISITED) {
        return;
    }

    ctx->visited_stack[ctx->visited_stack_depth++] = current_id;

    sqlite3_reset(ctx->stmt_calls);
    sqlite3_clear_bindings(ctx->stmt_calls);
    sqlite3_bind_int64(ctx->stmt_calls, 1, current_id);

    /* Collect child calls for this caller */
    typedef struct {
        int64_t target_id;
        char name[128];
        char file[256];
        int line;
    } child_call_t;

    child_call_t children[32];
    int child_count = 0;

    while (sqlite3_step(ctx->stmt_calls) == SQLITE_ROW && child_count < 32) {
        int64_t tid = sqlite3_column_int64(ctx->stmt_calls, 0);
        const char *tname = (const char *)sqlite3_column_text(ctx->stmt_calls, 1);
        const char *tfile = (const char *)sqlite3_column_text(ctx->stmt_calls, 3);
        int line = sqlite3_column_int(ctx->stmt_calls, 5);

        children[child_count].target_id = tid;
        snprintf(children[child_count].name, sizeof(children[child_count].name), "%s",
                 tname ? tname : "anon");
        snprintf(children[child_count].file, sizeof(children[child_count].file), "%s",
                 tfile ? tfile : "");
        children[child_count].line = line;
        child_count++;
        ctx->edges_traversed++;
    }

    for (int i = 0; i < child_count; i++) {
        if (ctx->trace->message_count >= SEQ_MAX_STEPS) {
            break;
        }

        ctx->nodes_analyzed++;
        int callee_participant_idx =
            get_or_add_participant(ctx, children[i].file, children[i].name);

        /* Add call message */
        cbm_seq_trace_add_message(ctx->trace, current_participant_idx, callee_participant_idx,
                                  children[i].name, children[i].line, depth + 1, false);

        /* Prevent infinite cycles */
        if (!is_in_stack(ctx, children[i].target_id)) {
            traverse_calls(ctx, children[i].target_id, children[i].name, children[i].file,
                           callee_participant_idx, depth + 1);
        }

        /* Add return message */
        if (current_participant_idx != callee_participant_idx) {
            cbm_seq_trace_add_message(ctx->trace, callee_participant_idx, current_participant_idx,
                                      "", 0, depth + 1, true);
        }
    }

    ctx->visited_stack_depth--;
}

int cbm_diagram_query_sequence(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                               cbm_seq_trace_t *trace, int *nodes_analyzed, int *edges_traversed,
                               char *errbuf, size_t errbuf_cap) {
    if (!store || !opts || !trace) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Invalid arguments to query_sequence");
        return -1;
    }

    const char *entry_point = opts->entry_point;
    if (!entry_point || entry_point[0] == '\0') {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Sequence diagram requires an entry_point");
        return -1;
    }

    const char *project = (opts->project && opts->project[0] != '\0') ? opts->project : "";
    sqlite3 *db = (sqlite3 *)cbm_store_get_db(store);
    if (!db) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Failed to get database handle from store");
        return -1;
    }

    /* 1. Find root entry point node */
    const char *sql_find =
        "SELECT id, name, qualified_name, file_path, start_line FROM nodes "
        "WHERE (project = ?1 OR ?1 = '') AND (qualified_name = ?2 OR name = ?2) "
        "ORDER BY (CASE WHEN qualified_name = ?2 THEN 0 ELSE 1 END), id ASC LIMIT 1;";

    sqlite3_stmt *stmt_root = NULL;
    int rc = sqlite3_prepare_v2(db, sql_find, -1, &stmt_root, NULL);
    if (rc != SQLITE_OK) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "SQL prepare failed: %s", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt_root, 1, project, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt_root, 2, entry_point, -1, SQLITE_STATIC);

    int64_t root_id = 0;
    char root_name[128] = "";
    char root_file[256] = "";

    if (sqlite3_step(stmt_root) == SQLITE_ROW) {
        root_id = sqlite3_column_int64(stmt_root, 0);
        const char *name = (const char *)sqlite3_column_text(stmt_root, 1);
        const char *file = (const char *)sqlite3_column_text(stmt_root, 3);
        snprintf(root_name, sizeof(root_name), "%s", name ? name : entry_point);
        snprintf(root_file, sizeof(root_file), "%s", file ? file : "");
    }
    sqlite3_finalize(stmt_root);

    /* If not found by exact name, try suffix search */
    if (root_id == 0) {
        const char *sql_suffix =
            "SELECT id, name, qualified_name, file_path, start_line FROM nodes "
            "WHERE (project = ?1 OR ?1 = '') AND (qualified_name LIKE '%' || ?2 OR name LIKE '%' || ?2) "
            "LIMIT 1;";
        rc = sqlite3_prepare_v2(db, sql_suffix, -1, &stmt_root, NULL);
        if (rc == SQLITE_OK) {
            sqlite3_bind_text(stmt_root, 1, project, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt_root, 2, entry_point, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt_root) == SQLITE_ROW) {
                root_id = sqlite3_column_int64(stmt_root, 0);
                const char *name = (const char *)sqlite3_column_text(stmt_root, 1);
                const char *file = (const char *)sqlite3_column_text(stmt_root, 3);
                snprintf(root_name, sizeof(root_name), "%s", name ? name : entry_point);
                snprintf(root_file, sizeof(root_file), "%s", file ? file : "");
            }
            sqlite3_finalize(stmt_root);
        }
    }

    if (root_id == 0) {
        if (errbuf) {
            snprintf(errbuf, errbuf_cap, "Entry point '%s' not found in project '%s'",
                     entry_point, project[0] ? project : "active");
        }
        return -1;
    }

    snprintf(trace->root_name, sizeof(trace->root_name), "%s", root_name);

    /* 2. Prepare statement for outgoing CALLS edges */
    const char *sql_calls =
        "SELECT e.target_id, n.name, n.qualified_name, n.file_path, n.start_line, "
        "       coalesce(json_extract(e.properties, '$.line'), json_extract(e.properties, '$.call_line'), n.start_line, 0) AS line_num "
        "FROM edges e "
        "JOIN nodes n ON e.target_id = n.id "
        "WHERE e.source_id = ?1 AND e.type = 'CALLS' "
        "ORDER BY line_num ASC, n.id ASC;";

    sqlite3_stmt *stmt_calls = NULL;
    rc = sqlite3_prepare_v2(db, sql_calls, -1, &stmt_calls, NULL);
    if (rc != SQLITE_OK) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "SQL prepare calls failed: %s", sqlite3_errmsg(db));
        return -1;
    }

    seq_query_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.db = db;
    ctx.stmt_calls = stmt_calls;
    ctx.trace = trace;
    ctx.max_depth = (opts->max_depth > 0 && opts->max_depth <= 8) ? opts->max_depth : 3;
    ctx.max_participants = (opts->max_participants > 0) ? opts->max_participants : 8;
    ctx.nodes_analyzed = 1;

    int root_p_idx = get_or_add_participant(&ctx, root_file, root_name);

    traverse_calls(&ctx, root_id, root_name, root_file, root_p_idx, 0);

    sqlite3_finalize(stmt_calls);

    if (nodes_analyzed) *nodes_analyzed = ctx.nodes_analyzed;
    if (edges_traversed) *edges_traversed = ctx.edges_traversed;

    return 0;
}
