/*
 * benchmark-fts-body.c — isolate the cost of the nodes_fts `body` column (#518).
 *
 * WHAT THIS MEASURES, AND WHY IT IS THE RIGHT QUESTION
 *
 * The full-index path already performed a wholesale FTS rebuild before #518:
 * delete-all followed by a full re-INSERT over every node (pipeline.c,
 * generation_rebuild_fts).  Adding `body` therefore does NOT introduce a new
 * pass over the graph — it makes an existing rebuild index more text.  So the
 * honest cost question is narrower than "how much slower is indexing":
 *
 *   1. per-row body tokenisation time, and
 *   2. the storage the extra column adds to the FTS index.
 *
 * This harness measures exactly those two, at a range of node counts, by
 * building the same synthetic corpus three ways:
 *
 *   A  4-column FTS (pre-#518 baseline: name, qualified_name, label, file_path)
 *   B  5-column FTS, body backfilled for EVERY node (what #518 ships)
 *   C  5-column FTS, body backfilled only for Section/Module rows
 *      (the "one-line WHERE" lever — loses function-docstring search)
 *
 * B minus A is the cost of the feature.  C minus A is the cost if the backfill
 * is narrowed.  B minus C is what the narrowing would save.
 *
 * WHAT IT DOES NOT MEASURE
 *
 * Real-corpus end-to-end wall-clock.  That needs a built product binary and a
 * real repository — use scripts/benchmark-index.sh for it.  This harness
 * deliberately isolates the FTS write so the numbers are not buried under
 * parsing, tree-sitter, and I/O.
 *
 * FIDELITY NOTES (read before trusting a number)
 *
 * - The real backfill wraps `name` in cbm_camel_split(); that function lives in
 *   the product, not here.  All three variants use the plain name identically,
 *   so it cancels out of every delta.  Absolute figures for the name column are
 *   therefore slightly low; the A/B/C deltas are unaffected.
 * - The body expression is the real one, json_valid() guard included, so
 *   malformed-JSON rows exercise the same path they do in production.
 * - Index size is measured as page_count * page_size after the backfill, minus
 *   the same figure for a database holding only the `nodes` table.  That
 *   isolates the FTS shadow tables from the base data.
 *
 * BUILD
 *   see scripts/benchmark-fts-body.sh, or:
 *   cc -O2 -o benchmark-fts-body scripts/benchmark-fts-body.c \
 *      vendored/sqlite3/sqlite3.c -Ivendored/sqlite3 \
 *      -DSQLITE_ENABLE_FTS5 -lpthread -lm
 *
 * USAGE
 *   ./benchmark-fts-body [rowcount ...]      (default: 100000 500000 2000000)
 */

#include "sqlite3.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
/* QueryPerformanceCounter rather than GetTickCount64: higher resolution, and it
 * does not require a Vista-era SDK (older MinGW headers lack the latter). */
static double now_ms(void) {
    LARGE_INTEGER f, t;
    QueryPerformanceFrequency(&f);
    QueryPerformanceCounter(&t);
    return (double)t.QuadPart * 1000.0 / (double)f.QuadPart;
}
#else
#include <time.h>
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}
#endif

/* ── Corpus shape ─────────────────────────────────────────────────────────
 * Tuned to the maintainer's estimate that Section+Module are ~5% of nodes, so
 * the narrow lever removes ~95% of the body backfill.  Adjust and re-run if
 * your corpus differs — every figure below is sensitive to these. */
enum {
    PCT_MODULE = 3,  /* one per indexed file */
    PCT_SECTION = 3, /* markdown headings */
    /* remainder: Function/Method/Class/... — the code symbols */

    PCT_CODE_HAS_DOC = 30,    /* share of code symbols carrying a docstring */
    PCT_SECTION_HAS_BODY = 80,/* share of Sections carrying prose */
    PCT_MODULE_HAS_DESC = 40, /* share of Modules carrying a description */
    PCT_MALFORMED = 1,        /* pre-fix rows whose properties JSON is invalid */

    DOC_LEN_CODE = 180,   /* average docstring length, bytes */
    DOC_LEN_SECTION = 300,/* markdown section bodies run longer */
    DOC_LEN_MODULE = 120,

    MAX_BODY = 500 /* MAX_COMMENT_LEN — the extractor's cap */
};

/* The real body expression from store.h (CBM_SQL_FTS_BODY_EXPR). */
#define BODY_EXPR                                                                                  \
    " CASE WHEN json_valid(properties)"                                                            \
    " THEN coalesce(json_extract(properties,'$.docstring'),'') ELSE '' END "

static const char *LABELS[] = {"Function", "Method", "Class", "Interface", "Route", "Variable"};
enum { N_LABELS = (int)(sizeof(LABELS) / sizeof(LABELS[0])) };

/* Deterministic PRNG so runs are reproducible across machines.  The state must be
 * a fixed-width 64-bit type: `unsigned long` is 32 bits on 32-bit targets, which
 * silently truncates the seed and degrades the xorshift period. */
static uint64_t rng_state = UINT64_C(88172645463325252);
static unsigned rnd(unsigned mod) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return (unsigned)(rng_state % mod);
}

/* Reset before each variant so all three see an identical corpus. */
static void rng_reset(void) { rng_state = UINT64_C(88172645463325252); }

static void die(sqlite3 *db, const char *what) {
    fprintf(stderr, "FATAL: %s: %s\n", what, db ? sqlite3_errmsg(db) : "(no db)");
    exit(1);
}

static void exec_or_die(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "FATAL: %s\n  while running: %s\n", err ? err : "?", sql);
        sqlite3_free(err);
        exit(1);
    }
}

/* Database size in bytes: page_count * page_size. */
static long long db_bytes(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    long long pages = 0, psize = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA page_count;", -1, &st, NULL) != SQLITE_OK) {
        die(db, "page_count");
    }
    if (sqlite3_step(st) == SQLITE_ROW) {
        pages = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st);
    if (sqlite3_prepare_v2(db, "PRAGMA page_size;", -1, &st, NULL) != SQLITE_OK) {
        die(db, "page_size");
    }
    if (sqlite3_step(st) == SQLITE_ROW) {
        psize = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st);
    return pages * psize;
}

/* Build one prose blob of roughly `avg` bytes from a small word pool.  Varied
 * vocabulary matters: FTS5 index size tracks distinct-term count, so a corpus
 * of one repeated word would understate the cost badly. */
static void make_prose(char *buf, size_t cap, int avg) {
    static const char *W[] = {"deployment", "rollback", "canary",   "pipeline",  "consumer",
                              "throughput", "schema",   "migration","validate",  "handler",
                              "retries",    "timeout",  "cursor",   "artifact",  "snapshot",
                              "buffer",     "resolve",  "template", "namespace", "session"};
    enum { NW = (int)(sizeof(W) / sizeof(W[0])) };
    int target = avg / 2 + (int)rnd((unsigned)avg); /* spread around the average */
    if (target > MAX_BODY) {
        target = MAX_BODY; /* the extractor caps before this ever reaches the store */
    }
    size_t n = 0;
    while (n < (size_t)target && n + 12 < cap) {
        const char *w = W[rnd(NW)];
        size_t wl = strlen(w);
        if (n + wl + 1 >= cap) {
            break;
        }
        memcpy(buf + n, w, wl);
        n += wl;
        buf[n++] = ' ';
    }
    buf[n] = '\0';
}

/* Populate a `nodes` table shaped like the product's, with a realistic mix. */
static void build_nodes(sqlite3 *db, long long rows) {
    exec_or_die(db, "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;");
    exec_or_die(db, "CREATE TABLE nodes ("
                    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "  project TEXT NOT NULL,"
                    "  label TEXT NOT NULL,"
                    "  name TEXT NOT NULL,"
                    "  qualified_name TEXT NOT NULL,"
                    "  file_path TEXT NOT NULL,"
                    "  properties TEXT DEFAULT '{}'"
                    ");");

    sqlite3_stmt *ins = NULL;
    if (sqlite3_prepare_v2(db,
                           "INSERT INTO nodes (project,label,name,qualified_name,file_path,"
                           "properties) VALUES (?1,?2,?3,?4,?5,?6)",
                           -1, &ins, NULL) != SQLITE_OK) {
        die(db, "prepare insert");
    }

    exec_or_die(db, "BEGIN;");
    char name[128], qn[256], fp[128], props[MAX_BODY + 64], prose[MAX_BODY + 32];
    for (long long i = 0; i < rows; i++) {
        unsigned roll = rnd(100);
        const char *label;
        int has_doc, doclen;
        if (roll < PCT_MODULE) {
            label = "Module";
            has_doc = (int)rnd(100) < PCT_MODULE_HAS_DESC;
            doclen = DOC_LEN_MODULE;
        } else if (roll < PCT_MODULE + PCT_SECTION) {
            label = "Section";
            has_doc = (int)rnd(100) < PCT_SECTION_HAS_BODY;
            doclen = DOC_LEN_SECTION;
        } else {
            label = LABELS[rnd(N_LABELS)];
            has_doc = (int)rnd(100) < PCT_CODE_HAS_DOC;
            doclen = DOC_LEN_CODE;
        }

        snprintf(name, sizeof(name), "symbol_%lld_handler", i);
        snprintf(fp, sizeof(fp), "src/pkg%u/mod%u.c", (unsigned)(i % 400), (unsigned)(i % 7919));
        snprintf(qn, sizeof(qn), "proj.pkg%u.%s", (unsigned)(i % 400), name);

        if ((int)rnd(100) < PCT_MALFORMED) {
            snprintf(props, sizeof(props), "{not valid json"); /* exercises json_valid() */
        } else if (has_doc) {
            make_prose(prose, sizeof(prose), doclen);
            snprintf(props, sizeof(props), "{\"docstring\":\"%s\"}", prose);
        } else {
            snprintf(props, sizeof(props), "{}");
        }

        sqlite3_bind_text(ins, 1, "proj", -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 2, label, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 3, name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 4, qn, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 5, fp, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 6, props, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) {
            die(db, "insert node");
        }
        sqlite3_reset(ins);
    }
    exec_or_die(db, "COMMIT;");
    sqlite3_finalize(ins);
}

typedef struct {
    double ms;
    long long index_bytes;
    long long bodies_indexed;
} result_t;

/* variant: 0 = A (4-col), 1 = B (body, all rows), 2 = C (body, Section/Module only) */
static result_t run_variant(const char *path, long long rows, int variant) {
    sqlite3 *db = NULL;
    remove(path);
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        die(db, "open");
    }
    rng_reset(); /* identical corpus for every variant — deltas must compare like with like */
    build_nodes(db, rows);
    long long base = db_bytes(db);

    if (variant == 0) {
        exec_or_die(db, "CREATE VIRTUAL TABLE nodes_fts USING fts5("
                        "  name, qualified_name, label, file_path,"
                        "  content='', tokenize='unicode61 remove_diacritics 2');");
    } else {
        exec_or_die(db, "CREATE VIRTUAL TABLE nodes_fts USING fts5("
                        "  name, qualified_name, label, file_path, body,"
                        "  content='', tokenize='unicode61 remove_diacritics 2');");
    }

    const char *sql;
    if (variant == 0) {
        sql = "INSERT INTO nodes_fts(rowid,name,qualified_name,label,file_path) "
              "SELECT id,name,qualified_name,label,file_path FROM nodes;";
    } else if (variant == 1) {
        sql = "INSERT INTO nodes_fts(rowid,name,qualified_name,label,file_path,body) "
              "SELECT id,name,qualified_name,label,file_path," BODY_EXPR "FROM nodes;";
    } else {
        /* The narrowing lever: only Section/Module rows contribute prose.  Every
         * node is still indexed by name — only the body is withheld. */
        sql = "INSERT INTO nodes_fts(rowid,name,qualified_name,label,file_path,body) "
              "SELECT id,name,qualified_name,label,file_path,"
              " CASE WHEN label IN ('Section','Module') AND json_valid(properties)"
              " THEN coalesce(json_extract(properties,'$.docstring'),'') ELSE '' END "
              "FROM nodes;";
    }

    double t0 = now_ms();
    exec_or_die(db, sql);
    double t1 = now_ms();

    result_t r;
    r.ms = t1 - t0;
    r.index_bytes = db_bytes(db) - base;

    /* How many rows actually contributed prose — the denominator for "cost per body". */
    const char *count_sql =
        (variant == 0)
            ? "SELECT 0;"
            : ((variant == 1)
                   ? "SELECT count(*) FROM nodes WHERE json_valid(properties) AND "
                     "coalesce(json_extract(properties,'$.docstring'),'') <> '';"
                   : "SELECT count(*) FROM nodes WHERE label IN ('Section','Module') AND "
                     "json_valid(properties) AND "
                     "coalesce(json_extract(properties,'$.docstring'),'') <> '';");
    sqlite3_stmt *st = NULL;
    r.bodies_indexed = 0;
    if (sqlite3_prepare_v2(db, count_sql, -1, &st, NULL) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) {
            r.bodies_indexed = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
    }

    sqlite3_close(db);
    remove(path);
    return r;
}

static double mb(long long bytes) { return (double)bytes / (1024.0 * 1024.0); }

/* Timing methodology.
 *
 * Two things matter here, and getting either wrong produces numbers that look
 * authoritative and are not:
 *
 * 1. Take the MINIMUM, not the mean.  Backfill time has a hard floor (the real
 *    work) and an unbounded tail (scheduler preemption, page-cache misses,
 *    thermal throttling).  Averaging folds that tail into the estimate; the
 *    minimum is the closest observable approximation of the floor.
 *
 * 2. INTERLEAVE the variants (A,B,C, A,B,C, ...) rather than running each to
 *    completion in turn (A,A,A, B,B,B, C,C,C).  Grouped runs let drift over the
 *    life of the process land entirely on whichever variant goes last — which
 *    on a loaded machine is enough to report variant C as *slower* than B even
 *    though C does strictly less work.  Interleaving spreads drift across all
 *    three so it cancels from the deltas.
 *
 * The observed spread (max/min) is printed so noise stays visible instead of
 * being quietly absorbed into a single confident-looking figure. */
enum { REPEATS = 3 };

typedef struct {
    result_t best;
    double worst_ms;
} sampled_t;

static void report(long long rows) {
    sampled_t s[3];
    for (int v = 0; v < 3; v++) {
        s[v].best.ms = 0;
        s[v].worst_ms = 0;
    }
    /* A fresh filename per (iteration, variant).  Reusing one path across
     * iterations is enough to hit "database is locked" on Windows, where the
     * previous handle can outlive close() briefly and remove() then silently
     * leaves the old file in place. */
    char path[64];

    for (int i = 0; i < REPEATS; i++) {
        for (int v = 0; v < 3; v++) {
            snprintf(path, sizeof(path), "bench_fts_%c_%d.db", (char)('a' + v), i);
            result_t r = run_variant(path, rows, v);
            if (i == 0 || r.ms < s[v].best.ms) {
                s[v].best = r;
            }
            if (i == 0 || r.ms > s[v].worst_ms) {
                s[v].worst_ms = r.ms;
            }
        }
    }
    result_t a = s[0].best, b = s[1].best, c = s[2].best;

    printf("\n== %lld nodes ==\n", rows);
    printf("  %-34s %10s %12s %12s\n", "variant", "backfill", "FTS index", "bodies");
    printf("  %-34s %9.0fms %10.1fMB %12lld\n", "A  4-column (pre-#518)", a.ms, mb(a.index_bytes),
           a.bodies_indexed);
    printf("  %-34s %9.0fms %10.1fMB %12lld\n", "B  +body, all nodes (#518)", b.ms,
           mb(b.index_bytes), b.bodies_indexed);
    printf("  %-34s %9.0fms %10.1fMB %12lld\n", "C  +body, Section/Module only", c.ms,
           mb(c.index_bytes), c.bodies_indexed);

    printf("  ----\n");
    printf("  B - A  cost of the feature      %+9.0fms %+10.1fMB  (%+.1f%% time, %+.1f%% size)\n",
           b.ms - a.ms, mb(b.index_bytes - a.index_bytes),
           a.ms > 0 ? (b.ms - a.ms) * 100.0 / a.ms : 0.0,
           a.index_bytes > 0
               ? (double)(b.index_bytes - a.index_bytes) * 100.0 / (double)a.index_bytes
               : 0.0);
    printf("  C - A  cost if narrowed         %+9.0fms %+10.1fMB\n", c.ms - a.ms,
           mb(c.index_bytes - a.index_bytes));
    printf("  B - C  what narrowing saves     %+9.0fms %+10.1fMB\n", b.ms - c.ms,
           mb(b.index_bytes - c.index_bytes));

    /* Surface the noise floor.  If the run-to-run spread is comparable to the
     * B-A delta being reported, the timing half of this table is not telling
     * you anything and needs a quieter machine or a larger row count. */
    double spread = 0.0;
    for (int v = 0; v < 3; v++) {
        double sp = s[v].best.ms > 0 ? (s[v].worst_ms - s[v].best.ms) * 100.0 / s[v].best.ms : 0.0;
        if (sp > spread) {
            spread = sp;
        }
    }
    printf("  run-to-run spread: %.0f%% (worst variant, %d runs)\n", spread, REPEATS);
    if (b.ms - a.ms > 0 && s[0].worst_ms - s[0].best.ms > (b.ms - a.ms) * 0.5) {
        printf("  !! WARNING: noise is large relative to the B-A delta — treat the timing\n"
               "     column as unreliable on this machine. Size figures are deterministic\n"
               "     and remain valid. Re-run on a quiet machine or with more rows.\n");
    }
    fflush(stdout);
}

int main(int argc, char **argv) {
    printf("nodes_fts `body` column cost isolation (#518)\n");
    printf("SQLite %s | corpus: %d%% Module, %d%% Section, rest code symbols\n",
           sqlite3_libversion(), PCT_MODULE, PCT_SECTION);
    printf("docstring coverage: code %d%%, Section %d%%, Module %d%%; %d%% malformed JSON\n",
           PCT_CODE_HAS_DOC, PCT_SECTION_HAS_BODY, PCT_MODULE_HAS_DESC, PCT_MALFORMED);
    printf("NOTE: cbm_camel_split() is not applied (it is product-side); identical across all\n"
           "      three variants, so it cancels from every delta below.\n");
    printf("timings: best of %d runs per variant\n", REPEATS);

    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            report(strtoll(argv[i], NULL, 10));
        }
    } else {
        report(100000);
        report(500000);
        report(2000000);
    }
    printf("\nExtrapolate to your corpus by scaling the B-A row; it is linear in node count.\n");
    return 0;
}
