/*
 * repro_harness.h — Shared helpers for cross-file / store-level / crash bug
 * reproductions (TIER A multi-file, TIER B crashes).
 *
 * Ported faithfully from the proven static harness in tests/test_lang_contract.c
 * so cross-file repro files don't each re-derive it. Header-only (static inline)
 * — each TU gets its own copy; no link conflicts. Include AFTER test_framework.h.
 *
 * Single-file extraction bugs do NOT need this — use cbm_extract_file directly
 * (see repro_extraction.c). Use this when the bug only appears once a fixture is
 * indexed through the full production pipeline (CALLS/IMPORTS/HTTP_CALLS edges,
 * cross-file/cross-package resolution, Route minting, dedup/upsert, etc.).
 */
#ifndef REPRO_HARNESS_H
#define REPRO_HARNESS_H

#include <foundation/compat.h>
#include "test_helpers.h" /* th_rmtree */
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h> /* cbm_project_name_from_path */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#if !defined(_WIN32)
#include <sys/wait.h> /* fork/waitpid crash isolation — POSIX only */
#endif

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} RProj;

typedef struct {
    const char *name; /* relative filename, may include '/' for subdirs */
    const char *content;
} RFile;

static inline void rh_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

/* Index lp->tmpdir (already populated) via the production index_repository flow
 * and open the resulting graph DB (NULL on failure). */
static inline cbm_store_t *rh_open_indexed(RProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project)
        return NULL;
    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv)
        return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp)
        free(resp);
    return cbm_store_open_path(lp->dbpath);
}

/* Write each fixture file into a fresh temp project, index it via the MCP
 * production flow, and open the resulting graph DB. Returns store (NULL on fail). */
static inline cbm_store_t *rh_index_files(RProj *lp, const RFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_repro_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    rh_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb"); /* binary: keep "\n" exact */
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return rh_open_indexed(lp);
}

static inline cbm_store_t *rh_index(RProj *lp, const char *filename, const char *content) {
    RFile f = {filename, content};
    return rh_index_files(lp, &f, 1);
}

static inline void rh_cleanup(RProj *lp, cbm_store_t *store) {
    if (store)
        cbm_store_close(store);
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* Count edges of a given type in the project graph. Returns -1 on query error. */
static inline int rh_count_edges(cbm_store_t *store, const char *project, const char *edge) {
    return store ? cbm_store_count_edges_by_type(store, project, edge) : -1;
}

/* Count nodes carrying `label`. Returns -1 on query error. */
static inline int rh_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* TIER B: returns true if cbm_extract_file CRASHES (signal) on `content`.
 * Runs in a forked child so the crash doesn't take down the repro runner. */
static inline bool rh_extract_crashes(const char *content, CBMLanguage lang, const char *relpath) {
#if defined(_WIN32)
    CBMFileResult *r =
        cbm_extract_file(content, (int)strlen(content), lang, "repro", relpath, 0, NULL, NULL);
    if (r)
        cbm_free_result(r);
    return false;
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        return false;
    if (pid == 0) {
        CBMFileResult *r =
            cbm_extract_file(content, (int)strlen(content), lang, "repro", relpath, 0, NULL, NULL);
        if (r)
            cbm_free_result(r);
        _exit(0);
    }
    int status = 0;
    (void)waitpid(pid, &status, 0);
    return WIFSIGNALED(status);
#endif
}

#endif /* REPRO_HARNESS_H */
