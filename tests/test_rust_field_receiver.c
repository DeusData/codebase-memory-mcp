/*
 * Issue #2605 — Rust field-receiver calls that fell to the registry's
 * name-suffix fallback.
 *
 * A method call through a structure field (`entry.session.push_daily()`) whose
 * receiver type the LSP layer could not infer never produced an LSP row. The
 * textual registry fallback then resolved by short name only, binding the call
 * to whichever same-named method sorted first: here the trait-declaration stub
 * `Analytic.push_daily` instead of `Session.push_daily`.
 *
 * The fix has two halves, and each is only exercisable on one pipeline path:
 *   - the shared Rust cross registry materialises the folded `field_defs`, and
 *     that registry is built ONLY on the parallel path (> 50 files);
 *   - the per-file path folds the same fields through its per-file registry.
 * So every assertion runs twice: sequential (CBM_WORKERS=1) and the parallel
 * twin padded past MIN_FILES_FOR_PARALLEL. Both directions are pinned — the
 * inherent method GAINS the caller and the trait stub does NOT — and the two
 * graphs must agree byte for byte on the CALLS edge set (the tie-break must
 * stay a pure function of the candidate set).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"

#include "mcp/mcp.h"
#include "pipeline/pipeline.h"
#include "sqlite3.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* MIN_FILES_FOR_PARALLEL is 50 (src/pipeline/pipeline.c); 56 fillers plus the
 * six repro files cross it. */
#define RF_PAD_FILES 56

static const char *RF_CARGO = "[package]\n"
                              "name = \"alpha\"\n"
                              "version = \"0.1.0\"\n"
                              "edition = \"2021\"\n"
                              "\n"
                              "[workspace]\n"
                              "members = [\"crates/alpha\"]\n";

/* The crate root re-exports the two names, so the caller's braced
 * `use crate::{Entry, Session};` resolves to a crate-rooted spelling that is
 * not the registered QN — the short-name probe has to recover it. */
static const char *RF_LIB = "pub mod analytic;\n"
                            "pub mod entry;\n"
                            "pub mod session;\n"
                            "pub use entry::Entry;\n"
                            "pub use session::Session;\n";

static const char *RF_SESSION = "pub struct Session {\n"
                                "    pub pushed: usize,\n"
                                "}\n"
                                "\n"
                                "impl Session {\n"
                                "    pub fn push_daily(&mut self) {\n"
                                "        self.pushed += 1;\n"
                                "    }\n"
                                "}\n";

/* The trait stub: a same-named declaration that the suffix bucket used to
 * prefer (it is lexicographically earlier than `session`). */
static const char *RF_ANALYTIC = "pub trait Analytic {\n"
                                 "    fn push_daily(&self);\n"
                                 "}\n";

/* Cross-file field: Entry holds a Session declared in another module. */
static const char *RF_ENTRY = "use crate::session::Session;\n"
                              "\n"
                              "pub struct Entry {\n"
                              "    pub session: Session,\n"
                              "}\n";

/* The caller: a field receiver whose type the file does not define. */
static const char *RF_STORE = "use crate::{Entry, Session};\n"
                              "\n"
                              "pub fn daily_from_store_at(entry: &mut Entry) {\n"
                              "    entry.session.push_daily();\n"
                              "}\n"
                              "\n"
                              "#[allow(dead_code)]\n"
                              "fn touch(_: Session) {}\n";

static int rf_write(const char *root, const char *rel, const char *text) {
    return th_write_file(TH_PATH(root, rel), text);
}

/* Write the repro plus enough filler Rust files to cross the parallel
 * threshold. Returns 0 on success. */
static int rf_build_fixture(const char *root) {
    if (rf_write(root, "Cargo.toml", RF_CARGO) != 0 ||
        rf_write(root, "crates/alpha/src/lib.rs", RF_LIB) != 0 ||
        rf_write(root, "crates/alpha/src/session.rs", RF_SESSION) != 0 ||
        rf_write(root, "crates/alpha/src/analytic.rs", RF_ANALYTIC) != 0 ||
        rf_write(root, "crates/alpha/src/entry.rs", RF_ENTRY) != 0 ||
        rf_write(root, "crates/alpha/src/store.rs", RF_STORE) != 0) {
        return -1;
    }
    char rel[128];
    char body[512];
    for (int i = 0; i < RF_PAD_FILES; i++) {
        snprintf(body, sizeof(body),
                 "pub struct Pad%d {\n"
                 "    pub n: usize,\n"
                 "}\n"
                 "\n"
                 "impl Pad%d {\n"
                 "    pub fn bump(&mut self) {\n"
                 "        self.n += 1;\n"
                 "    }\n"
                 "}\n"
                 "\n"
                 "pub fn pad_fn_%d(p: &mut Pad%d) {\n"
                 "    p.bump();\n"
                 "}\n",
                 i, i, i, i);
        snprintf(rel, sizeof(rel), "crates/alpha/src/pad_%02d.rs", i);
        if (rf_write(root, rel, body) != 0) {
            return -1;
        }
    }
    return 0;
}

static int rf_index(const char *root, const char *db) {
    cbm_pipeline_t *p = cbm_pipeline_new(root, db, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    return rc;
}

static void rf_unlink_db(const char *db) {
    char side[600];
    unlink(db);
    snprintf(side, sizeof(side), "%s-wal", db);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", db);
    unlink(side);
}

/* Number of CALLS edges from a definition whose QN ends with `caller_suffix`
 * to one ending with `callee_suffix`; -1 when the database cannot be read. */
static int rf_calls(const char *db, const char *caller_suffix, const char *callee_suffix) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT COUNT(*) FROM edges e "
                          "JOIN nodes s ON s.id = e.source_id "
                          "JOIN nodes t ON t.id = e.target_id "
                          "WHERE e.type = 'CALLS' AND s.qualified_name LIKE '%.' || ?1 "
                          "AND t.qualified_name LIKE '%.' || ?2";
        if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, caller_suffix, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, callee_suffix, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                n = sqlite3_column_int(st, 0);
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

/* Canonical text of every CALLS edge whose target is one of the two
 * same-named methods: the byte-identical gate between the two paths. The
 * caller frees it; NULL when the database cannot be read. */
static char *rf_call_state(const char *db) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return NULL;
    }
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    buf[0] = '\0';
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT s.qualified_name || ' -> ' || t.qualified_name "
                      "FROM edges e JOIN nodes s ON s.id = e.source_id "
                      "JOIN nodes t ON t.id = e.target_id "
                      "WHERE e.type = 'CALLS' AND (t.qualified_name LIKE '%.push_daily') "
                      "ORDER BY 1";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *line = (const char *)sqlite3_column_text(st, 0);
            size_t l = line ? strlen(line) : 0;
            if (len + l + 2 > cap) {
                cap = (len + l + 2) * 2;
                buf = realloc(buf, cap);
            }
            if (l) {
                memcpy(buf + len, line, l);
                len += l;
            }
            buf[len++] = '\n';
            buf[len] = '\0';
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return buf;
}

/* Both directions on one index: the inherent method gains the field-receiver
 * caller and the trait stub does not. Returns 0, or -1 after printing
 * `label` and the measured counts. */
static int rf_check_path(const char *label, const char *db) {
    int inherent = rf_calls(db, "daily_from_store_at", "Session.push_daily");
    int stub = rf_calls(db, "daily_from_store_at", "Analytic.push_daily");
    if (inherent != 1 || stub != 0) {
        printf("  %s: daily_from_store_at -> Session.push_daily = %d (want 1), "
               "-> Analytic.push_daily = %d (want 0)\n",
               label, inherent, stub);
        return -1;
    }
    return 0;
}

TEST(rust_field_receiver_both_resolver_paths) {
    char tmp[256] = "/tmp/cbm_rust_field_XXXXXX";
    ASSERT_TRUE(cbm_mkdtemp(tmp));
    ASSERT_EQ(rf_build_fixture(tmp), 0);

    char seq_db[600], par_db[600];
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);

    /* One worker: the sequential extraction path (per-file registry). */
    const char *saved = getenv("CBM_WORKERS");
    char *saved_copy = saved ? strdup(saved) : NULL;

    cbm_setenv("CBM_WORKERS", "1", 1);
    int seq_rc = rf_index(tmp, seq_db);
    /* Four workers + > 50 files: the parallel path, and therefore the shared
     * Tier-2 Rust registry this fix must populate with fields. */
    cbm_setenv("CBM_WORKERS", "4", 1);
    int par_rc = rf_index(tmp, par_db);

    if (saved_copy) {
        cbm_setenv("CBM_WORKERS", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }

    ASSERT_EQ(seq_rc, 0);
    ASSERT_EQ(par_rc, 0);

    int seq_ok = rf_check_path("sequential", seq_db);
    int par_ok = rf_check_path("parallel", par_db);

    /* Determinism: both paths must publish the identical CALLS edges onto the
     * two same-named methods — the tie-break is a pure function of the
     * candidate set, not of worker scheduling. */
    char *seq_state = rf_call_state(seq_db);
    char *par_state = rf_call_state(par_db);
    bool same = seq_state && par_state && strcmp(seq_state, par_state) == 0;
    if (!same) {
        printf("  sequential != parallel\n--- sequential\n%s--- parallel\n%s",
               seq_state ? seq_state : "(null)", par_state ? par_state : "(null)");
    }
    free(seq_state);
    free(par_state);

    rf_unlink_db(seq_db);
    rf_unlink_db(par_db);
    th_rmtree(tmp);

    ASSERT_EQ(seq_ok, 0);
    ASSERT_EQ(par_ok, 0);
    ASSERT_TRUE(same);
    PASS();
}

void suite_rust_field_receiver(void) {
    RUN_TEST(rust_field_receiver_both_resolver_paths);
}
