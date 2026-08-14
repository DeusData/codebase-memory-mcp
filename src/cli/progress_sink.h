/*
 * progress_sink.h — Human-readable progress for one-shot CLI commands.
 *
 * Installs a log sink that maps structured pipeline events to phase labels.
 * Interactive terminals enable it automatically; --progress forces it when
 * stderr is redirected, and --quiet disables it.
 * Usage:
 *   cbm_progress_sink_init(stderr);
 *   // ... run pipeline ...
 *   cbm_progress_sink_fini();
 */
#ifndef CBM_PROGRESS_SINK_H
#define CBM_PROGRESS_SINK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
    bool quiet_requested;
    bool progress_requested;
    bool verbose_requested;
} cbm_cli_output_flags_t;

/* Parse process-level output controls without consuming a tool's own
 * `--verbose` argument. The argv array is compacted in place. */
bool cbm_cli_output_flags_parse(int *argc, char **argv, cbm_cli_output_flags_t *flags, char *error,
                                size_t error_size);

/* Apply process-level diagnostic verbosity without changing stdout. */
void cbm_cli_diagnostics_configure(bool quiet_requested, bool verbose_requested);

/* Interactive terminals get lifecycle feedback automatically. --progress
 * forces the same behavior for redirected stderr; --quiet disables both. */
bool cbm_cli_progress_enabled(bool explicitly_requested, bool quiet_requested, bool stderr_is_tty);
void cbm_cli_progress_start(FILE *out, const char *tool_name);
void cbm_cli_progress_finish(FILE *out, const char *tool_name, bool success, uint64_t elapsed_ms);

void cbm_progress_sink_init(FILE *out);
void cbm_progress_sink_fini(void);
void cbm_progress_sink_fn(const char *line);

#endif
