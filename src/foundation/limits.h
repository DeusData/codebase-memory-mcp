/*
 * limits.h — Generous, env-configurable safety limits (Stage 2 / Track B4).
 *
 * Each knob has a generous default. Hitting a limit degrades to a *reported*
 * skip (surfaced via MCP/CLI/logfile), never a silent drop and never an
 * unbounded read (unbounded just trades a crash for an OOM/hang). Every limit
 * is env-overridable so an operator can tune it per-repo without a rebuild.
 */
#ifndef CBM_LIMITS_H
#define CBM_LIMITS_H

/* Result of an attempted per-file read, so callers can attribute a skip to the
 * right phase/reason instead of collapsing every failure into "read failed". */
typedef enum {
    CBM_READ_OK = 0,    /* file read successfully */
    CBM_READ_OPEN_FAIL, /* could not open (missing / permission) */
    CBM_READ_EMPTY,     /* zero/negative size — benign, nothing to index */
    CBM_READ_OVERSIZED, /* size exceeds cbm_max_file_bytes() */
    CBM_READ_OOM,       /* buffer allocation failed */
} cbm_read_status_t;

/* Maximum size (bytes) of a single source file the indexer will read into
 * memory. Files larger than this are skipped-and-reported (phase "oversized"),
 * never silently dropped. Override with CBM_MAX_FILE_BYTES (a positive integer
 * count of bytes). Default 512 MiB (raised from the historical 100 MB cap).
 *
 * The env var is read on each call — this is intentional: read_file() calls it
 * once per file (negligible), and reading fresh means a test / operator can
 * change the cap via setenv without a process restart or a stale memoized copy
 * leaking across runs. */
long cbm_max_file_bytes(void);

#endif /* CBM_LIMITS_H */
