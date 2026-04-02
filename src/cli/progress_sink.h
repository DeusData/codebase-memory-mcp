/*
 * progress_sink.h — Human-readable progress output for --progress CLI flag.
 *
 * Installs a log sink that maps structured pipeline events to phase labels.
 * Usage:
 *   cbm_progress_sink_init(stderr);
 *   // ... run pipeline ...
 *   cbm_progress_sink_fini();
 */
#ifndef CBM_PROGRESS_SINK_H
#define CBM_PROGRESS_SINK_H

#include <stdio.h>

void cbm_progress_sink_init(FILE *out);
void cbm_progress_sink_fini(void);
void cbm_progress_sink_fn(const char *line);

#endif
