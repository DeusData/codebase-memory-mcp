/*
 * progress_sink.h — Human-readable progress sink for the --progress CLI flag.
 *
 * Installs a cbm_log_sink_fn that maps structured log events emitted by the
 * indexing pipeline to human-readable phase labels printed to stderr.
 *
 * Usage:
 *   cbm_progress_sink_init(stderr);     // before cbm_pipeline_run()
 *   cbm_pipeline_run(p);
 *   cbm_progress_sink_fini();           // after run; restores previous sink
 */
#ifndef CBM_PROGRESS_SINK_H
#define CBM_PROGRESS_SINK_H

#include <stdio.h>

/* Install the progress sink.  out should be stderr.
 * Saves the previously-registered sink so it can be restored by _fini. */
void cbm_progress_sink_init(FILE *out);

/* Uninstall the progress sink.
 * Restores the previous sink and emits a trailing newline if needed. */
void cbm_progress_sink_fini(void);

/* The log-sink callback (cbm_log_sink_fn signature).
 * Parses msg= tag from structured log lines and prints phase labels to stderr.
 * Thread-safe: may be called from worker threads during parallel extract. */
void cbm_progress_sink_fn(const char *line);

#endif /* CBM_PROGRESS_SINK_H */
