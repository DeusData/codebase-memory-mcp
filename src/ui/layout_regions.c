/*
 * layout_regions.c — CBM Atlas region level (level=regions).
 *
 * The galaxy's coarsest level of detail: one body per REGION instead of one
 * per node, so a project of any size opens instantly (#498). A region is a
 * set of FILES — every node inherits its file's region — assigned in order
 * of preference:
 *
 *   1. Leiden communities over the CALLS graph (the de-facto architecture),
 *      voted per file, top communities kept;
 *   2. folder groups (first two path components) for files the communities
 *      do not explain — and for very large graphs where Leiden is skipped;
 *   3. a final "misc" bucket so every file belongs somewhere.
 *
 * Everything here is deterministic (Leiden is seeded, ranking ties break by
 * id) and explained: each region carries the reason it got its name. Results
 * are cached per (project, indexed_at) so the region scene and the region
 * scopes reuse one computation.
 */
#include "foundation/constants.h"
#include "foundation/json_writer.h"
#include "ui/layout_internal.h"
#include "ui/atlas.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Tunables ─────────────────────────────────────────────────── */

enum {
    REGION_LEIDEN_NODE_CAP = 200000, /* above this, folder regions only */
    REGION_MAX_LEIDEN = 24,          /* Leiden regions kept (by file count) */
    REGION_MAX_TOTAL = 40,           /* hard cap on regions overall */
    REGION_MIN_FILES = 2,            /* a Leiden region must span ≥2 files */
    REGION_FOLDER_MIN_FILES = 3,     /* a folder group must have ≥3 files */
    REGION_TOP_NODES = 5,            /* representative names per region */
    REGION_EDGE_PULL_CAP = 6,        /* max layout-edge replication per pair */
    REGION_SEED_RADIUS = 600,
};

/* ── Per-file record (interned by file_path) ──────────────────── */

typedef struct {
    int idx;       /* dense file index */
    int region;    /* final region id, -1 until assigned */
    int community; /* winning Leiden community, -1 if none */
    int votes;     /* votes for the winning community so far */
} file_rec_t;

typedef struct {
    char *name; /* display name (dominant folder key, or the key itself) */
    char *hub;  /* highest-degree member callable (may be NULL) */
    char *why;  /* one-line provenance for the name/grouping */
    int files;
    long long members;
    long long internal_edges;
    long long boundary_edges;
    char *top_nodes[REGION_TOP_NODES];
    int top_count;
} region_t;

/* ── Cache: one entry, guarded — the daemon serves one UI ─────── */

typedef struct {
    char key[1152];
    char *json; /* serialized regions payload */
    int region_count;
    char ***region_files; /* per region: owned file_path strings */
    int *region_file_count;
    long long total_nodes;
    CBMHashTable *file_ht; /* file_path (borrowed from region_files) → region+1 */
} region_cache_t;

static region_cache_t g_region_cache;
static pthread_mutex_t g_region_mu = PTHREAD_MUTEX_INITIALIZER;

static void region_cache_clear_locked(void) {
    cbm_free(CBM_MEM_CLASS_OTHER, g_region_cache.json);
    for (int r = 0; r < g_region_cache.region_count; r++) {
        for (int f = 0; f < g_region_cache.region_file_count[r]; f++)
            cbm_free(CBM_MEM_CLASS_OTHER, g_region_cache.region_files[r][f]);
        cbm_free(CBM_MEM_CLASS_OTHER, g_region_cache.region_files[r]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, g_region_cache.region_files);
    cbm_free(CBM_MEM_CLASS_OTHER, g_region_cache.region_file_count);
    cbm_ht_free(g_region_cache.file_ht);
    memset(&g_region_cache, 0, sizeof(g_region_cache));
}

void cbm_layout_regions_cache_clear(void) {
    pthread_mutex_lock(&g_region_mu);
    region_cache_clear_locked();
    pthread_mutex_unlock(&g_region_mu);
}

/* Freshness key: the project's indexed_at stamp (updated on every index
 * write). A missing projects row hashes as the empty string. */
static void region_cache_key(cbm_store_t *store, const char *project, char *out, size_t cap) {
    char stamp[128] = {0};
    struct sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *st = NULL;
    if (db && sqlite3_prepare_v2(db, "SELECT indexed_at FROM projects WHERE name=?1", -1, &st,
                                 NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *t = (const char *)sqlite3_column_text(st, 0);
            if (t)
                snprintf(stamp, sizeof(stamp), "%s", t);
        }
        sqlite3_finalize(st);
    }
    snprintf(out, cap, "%s|%s", project, stamp);
}

/* ── Small helpers ────────────────────────────────────────────── */

static char *rg_strdup(const char *s) {
    if (!s)
        return NULL;
    size_t len = strlen(s) + 1;
    char *copy = cbm_alloc(CBM_MEM_CLASS_OTHER, len);
    if (copy)
        memcpy(copy, s, len);
    return copy;
}

/* Folder key: the first two path components ("src/pipeline"), one for a
 * top-level directory, "." for repo-root files. */
static void rg_folder_key(const char *path, char *out, size_t cap) {
    if (!path || !path[0]) {
        snprintf(out, cap, ".");
        return;
    }
    const char *first = strchr(path, '/');
    if (!first) {
        snprintf(out, cap, ".");
        return;
    }
    const char *second = strchr(first + 1, '/');
    size_t len = second ? (size_t)(second - path) : (size_t)(first - path);
    if (len >= cap)
        len = cap - 1;
    memcpy(out, path, len);
    out[len] = '\0';
}

typedef struct {
    int64_t id;
    int32_t region;
} rg_id_region_t;

static int rg_id_region_cmp(const void *a, const void *b) {
    int64_t ia = ((const rg_id_region_t *)a)->id;
    int64_t ib = ((const rg_id_region_t *)b)->id;
    return (ia > ib) - (ia < ib);
}

static int32_t rg_region_of(const rg_id_region_t *map, long long n, int64_t id) {
    long long lo = 0, hi = n - 1;
    while (lo <= hi) {
        long long mid = lo + (hi - lo) / 2;
        if (map[mid].id == id)
            return map[mid].region;
        if (map[mid].id < id)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return -1;
}

/* Region colors: Okabe-Ito (color-blind safe) for the first ten regions —
 * ranking already orders regions by size, so the vivid hues land on the
 * regions that matter — and a neutral gray for the tail, where labels and
 * position carry identity. Beyond ~10 hues distinguishability collapses
 * anyway (Okabe & Ito; Tol). */
static uint32_t rg_wheel_color(int index, int count) {
    (void)count;
    static const uint32_t okabe_ito[10] = {
        0xE69F00, /* orange */
        0x56B4E9, /* sky */
        0x009E73, /* bluish green */
        0xF0E442, /* yellow */
        0x4393C9, /* blue, lightened for dark grounds */
        0xD55E00, /* vermilion */
        0xCC79A7, /* reddish purple */
        0xEE3377, /* magenta (Tol) */
        0x33BBEE, /* cyan (Tol) */
        0x999999, /* gray */
    };
    if (index >= 0 && index < 10)
        return okabe_ito[index];
    return 0x6E7A87; /* neutral tail — labels do the work */
}

/* ── Leiden vote: (file_idx, community) pairs → per-file winner ── */

typedef struct {
    int file_idx;
    int community;
} rg_vote_t;

static int rg_vote_cmp(const void *a, const void *b) {
    const rg_vote_t *va = a, *vb = b;
    if (va->file_idx != vb->file_idx)
        return va->file_idx - vb->file_idx;
    return va->community - vb->community;
}

/* ── Region build ─────────────────────────────────────────────── */

typedef struct {
    region_t *regions;
    int region_count;
    long long *edge_w; /* dense region_count × region_count weights */
    rg_id_region_t *node_map;
    long long node_map_count;
    long long total_nodes;
    long long unmapped_nodes;
    const char *method; /* "leiden+folders" or "folders" */
    /* per-file bookkeeping for the cache */
    char **file_paths;
    file_rec_t **file_recs;
    int file_count;
} rg_build_t;

static void rg_build_free(rg_build_t *b) {
    if (!b)
        return;
    for (int r = 0; r < b->region_count; r++) {
        cbm_free(CBM_MEM_CLASS_OTHER, b->regions[r].name);
        cbm_free(CBM_MEM_CLASS_OTHER, b->regions[r].hub);
        cbm_free(CBM_MEM_CLASS_OTHER, b->regions[r].why);
        for (int t = 0; t < b->regions[r].top_count; t++)
            cbm_free(CBM_MEM_CLASS_OTHER, b->regions[r].top_nodes[t]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, b->regions);
    cbm_free(CBM_MEM_CLASS_OTHER, b->edge_w);
    cbm_free(CBM_MEM_CLASS_OTHER, b->node_map);
    for (int f = 0; f < b->file_count; f++) {
        cbm_free(CBM_MEM_CLASS_OTHER, b->file_paths[f]);
        cbm_free(CBM_MEM_CLASS_OTHER, b->file_recs[f]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, b->file_paths);
    cbm_free(CBM_MEM_CLASS_OTHER, b->file_recs);
    memset(b, 0, sizeof(*b));
}

/* Intern a file path; returns its record (creating one on first sight). */
static file_rec_t *rg_file_intern(CBMHashTable *ht, rg_build_t *b, int *cap, const char *path) {
    file_rec_t *rec = cbm_ht_get(ht, path);
    if (rec)
        return rec;
    if (b->file_count >= *cap) {
        int nc = *cap ? *cap * 2 : 1024;
        char **np = cbm_realloc(CBM_MEM_CLASS_OTHER, b->file_paths, (size_t)nc * sizeof(char *));
        file_rec_t **nr =
            cbm_realloc(CBM_MEM_CLASS_OTHER, b->file_recs, (size_t)nc * sizeof(file_rec_t *));
        if (!np || !nr) {
            cbm_free(CBM_MEM_CLASS_OTHER, np ? np : b->file_paths);
            b->file_paths = np ? np : NULL;
            if (nr)
                b->file_recs = nr;
            return NULL;
        }
        b->file_paths = np;
        b->file_recs = nr;
        *cap = nc;
    }
    rec = cbm_atlas_calloc_array(1, sizeof(*rec));
    char *owned = rg_strdup(path);
    if (!rec || !owned) {
        cbm_free(CBM_MEM_CLASS_OTHER, rec);
        cbm_free(CBM_MEM_CLASS_OTHER, owned);
        return NULL;
    }
    rec->idx = b->file_count;
    rec->region = -1;
    rec->community = -1;
    b->file_paths[b->file_count] = owned;
    b->file_recs[b->file_count] = rec;
    b->file_count++;
    cbm_ht_set(ht, owned, rec);
    return rec;
}

/* Rank helper: (key index, file count) sorted by count desc, index asc. */
typedef struct {
    int key;
    int files;
} rg_rank_t;

static int rg_rank_cmp(const void *a, const void *b) {
    const rg_rank_t *ra = a, *rb = b;
    if (rb->files != ra->files)
        return rb->files - ra->files;
    return ra->key - rb->key;
}

/* Assign regions to files. Returns 0 on success. Fills b->regions (names,
 * files counts, why) and every file's rec->region. */
static int rg_assign_regions(cbm_store_t *store, const char *project, CBMHashTable *file_ht,
                             rg_build_t *b) {
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return -1;

    /* 1. Callable nodes → files + Leiden input. */
    int cap = 0;
    int64_t *cids = NULL;
    int *cfile = NULL; /* file idx per callable */
    char **cnames = NULL;
    int cn = 0, ccap = 0;
    sqlite3_stmt *st = NULL;
    const char *nsql =
        "SELECT id, name, file_path FROM nodes WHERE project=?1 AND label IN "
        "(" CBM_SQL_CALLABLE_OR_TYPE_LABELS ") AND file_path IS NOT NULL ORDER BY id LIMIT ?2";
    if (sqlite3_prepare_v2(db, nsql, -1, &st, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, REGION_LEIDEN_NODE_CAP + 1);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *path = (const char *)sqlite3_column_text(st, 2);
        if (!path || !path[0])
            continue;
        file_rec_t *rec = rg_file_intern(file_ht, b, &cap, path);
        if (!rec)
            break;
        if (cn >= ccap) {
            int nc = ccap ? ccap * 2 : 4096;
            int64_t *ni = cbm_realloc(CBM_MEM_CLASS_OTHER, cids, (size_t)nc * sizeof(int64_t));
            int *nf = cbm_realloc(CBM_MEM_CLASS_OTHER, cfile, (size_t)nc * sizeof(int));
            char **nn = cbm_realloc(CBM_MEM_CLASS_OTHER, cnames, (size_t)nc * sizeof(char *));
            if (!ni || !nf || !nn) {
                cbm_free(CBM_MEM_CLASS_OTHER, ni ? ni : cids);
                cbm_free(CBM_MEM_CLASS_OTHER, nf ? nf : cfile);
                cbm_free(CBM_MEM_CLASS_OTHER, nn ? nn : cnames);
                cids = NULL;
                cfile = NULL;
                cnames = NULL;
                cn = 0;
                break;
            }
            cids = ni;
            cfile = nf;
            cnames = nn;
            ccap = nc;
        }
        cids[cn] = sqlite3_column_int64(st, 0);
        cfile[cn] = rec->idx;
        cnames[cn] = rg_strdup((const char *)sqlite3_column_text(st, 1));
        cn++;
    }
    sqlite3_finalize(st);

    bool use_leiden = cn > 0 && cn <= REGION_LEIDEN_NODE_CAP;
    int *ccomm = NULL; /* community per callable */
    int community_count = 0;
    int *cdeg = cbm_atlas_calloc_array(cn > 0 ? (size_t)cn : 1, sizeof(int));

    if (use_leiden && cdeg) {
        /* CALLS edges among the callable set. */
        rg_id_region_t *idmap = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)cn * sizeof(rg_id_region_t));
        cbm_louvain_edge_t *ledges = NULL;
        int le = 0, lcap = 0;
        if (idmap) {
            for (int i = 0; i < cn; i++) {
                idmap[i].id = cids[i];
                idmap[i].region = i;
            }
            qsort(idmap, (size_t)cn, sizeof(rg_id_region_t), rg_id_region_cmp);
            sqlite3_stmt *est = NULL;
            if (sqlite3_prepare_v2(db,
                                   "SELECT source_id, target_id FROM edges WHERE project=?1 AND "
                                   "type='CALLS'",
                                   -1, &est, NULL) == SQLITE_OK) {
                sqlite3_bind_text(est, 1, project, -1, SQLITE_STATIC);
                while (sqlite3_step(est) == SQLITE_ROW) {
                    int si = rg_region_of(idmap, cn, sqlite3_column_int64(est, 0));
                    int ti = rg_region_of(idmap, cn, sqlite3_column_int64(est, 1));
                    if (si < 0 || ti < 0 || si == ti)
                        continue;
                    if (le >= lcap) {
                        int nc = lcap ? lcap * 2 : 4096;
                        cbm_louvain_edge_t *ne =
                            cbm_realloc(CBM_MEM_CLASS_OTHER, ledges, (size_t)nc * sizeof(*ledges));
                        if (!ne)
                            break;
                        ledges = ne;
                        lcap = nc;
                    }
                    ledges[le].src = cids[si];
                    ledges[le].dst = cids[ti];
                    le++;
                    cdeg[si]++;
                    cdeg[ti]++;
                }
                sqlite3_finalize(est);
            }
        }
        cbm_louvain_result_t *res = NULL;
        int rn = 0;
        if (idmap && cbm_leiden(cids, cn, ledges, le, 1.0, &res, &rn) == CBM_STORE_OK && res &&
            rn == cn) {
            ccomm = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)cn * sizeof(int));
            if (ccomm) {
                for (int i = 0; i < cn; i++) {
                    ccomm[i] = res[i].community;
                    if (ccomm[i] + 1 > community_count)
                        community_count = ccomm[i] + 1;
                }
            }
        }
        cbm_free(CBM_MEM_CLASS_OTHER, res);
        cbm_free(CBM_MEM_CLASS_OTHER, ledges);
        cbm_free(CBM_MEM_CLASS_OTHER, idmap);
    }

    /* 2. Per-file community vote (majority; ties break on lower community). */
    if (ccomm && community_count > 0 && b->file_count > 0) {
        rg_vote_t *votes = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)cn * sizeof(rg_vote_t));
        if (votes) {
            for (int i = 0; i < cn; i++) {
                votes[i].file_idx = cfile[i];
                votes[i].community = ccomm[i];
            }
            qsort(votes, (size_t)cn, sizeof(rg_vote_t), rg_vote_cmp);
            int i = 0;
            while (i < cn) {
                int f = votes[i].file_idx;
                int best_comm = -1, best_votes = 0;
                int j = i;
                while (j < cn && votes[j].file_idx == f) {
                    int comm = votes[j].community;
                    int k = j;
                    while (k < cn && votes[k].file_idx == f && votes[k].community == comm)
                        k++;
                    if (k - j > best_votes) {
                        best_votes = k - j;
                        best_comm = comm;
                    }
                    j = k;
                }
                b->file_recs[f]->community = best_comm;
                b->file_recs[f]->votes = best_votes;
                i = j;
            }
            cbm_free(CBM_MEM_CLASS_OTHER, votes);
        }
    }

    /* 3. Community → file counts; keep the biggest as Leiden regions. */
    int *comm_files =
        community_count > 0 ? cbm_atlas_calloc_array((size_t)community_count, sizeof(int)) : NULL;
    if (comm_files) {
        for (int f = 0; f < b->file_count; f++)
            if (b->file_recs[f]->community >= 0)
                comm_files[b->file_recs[f]->community]++;
    }
    int *comm_region = community_count > 0
                           ? cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)community_count * sizeof(int))
                           : NULL;
    if (comm_region)
        for (int c = 0; c < community_count; c++)
            comm_region[c] = -1;

    region_t *regions = cbm_atlas_calloc_array(REGION_MAX_TOTAL, sizeof(region_t));
    int region_count = 0;
    if (!regions) {
        cbm_free(CBM_MEM_CLASS_OTHER, comm_files);
        cbm_free(CBM_MEM_CLASS_OTHER, comm_region);
        for (int i = 0; i < cn; i++)
            cbm_free(CBM_MEM_CLASS_OTHER, cnames[i]);
        cbm_free(CBM_MEM_CLASS_OTHER, cnames);
        cbm_free(CBM_MEM_CLASS_OTHER, cids);
        cbm_free(CBM_MEM_CLASS_OTHER, cfile);
        cbm_free(CBM_MEM_CLASS_OTHER, cdeg);
        cbm_free(CBM_MEM_CLASS_OTHER, ccomm);
        return -1;
    }

    if (comm_files && comm_region) {
        rg_rank_t *rank =
            cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)community_count * sizeof(rg_rank_t));
        if (rank) {
            for (int c = 0; c < community_count; c++)
                rank[c] = (rg_rank_t){c, comm_files[c]};
            qsort(rank, (size_t)community_count, sizeof(rg_rank_t), rg_rank_cmp);
            for (int r = 0; r < community_count && region_count < REGION_MAX_LEIDEN; r++) {
                if (rank[r].files < REGION_MIN_FILES)
                    break;
                comm_region[rank[r].key] = region_count;
                regions[region_count].files = rank[r].files;
                region_count++;
            }
            cbm_free(CBM_MEM_CLASS_OTHER, rank);
        }
    }

    /* Assign files covered by kept communities. */
    for (int f = 0; f < b->file_count; f++) {
        int comm = b->file_recs[f]->community;
        if (comm >= 0 && comm_region && comm_region[comm] >= 0)
            b->file_recs[f]->region = comm_region[comm];
    }
    int leiden_regions = region_count;

    /* 4. Folder groups for everything still unassigned. Files seen so far are
     * only those with callables; other files join in the full node pass by
     * inheriting their folder group (interned there). Here we pre-create
     * folder regions from the already-known unassigned files; the "misc"
     * region catches the rest. */
    CBMHashTable *folder_ht = cbm_ht_create(256); /* key → 1-based folder slot */
    char **folder_keys = NULL;
    int *folder_files = NULL;
    int folder_count = 0, folder_cap = 0;
    if (folder_ht) {
        for (int f = 0; f < b->file_count; f++) {
            if (b->file_recs[f]->region >= 0)
                continue;
            char key[CBM_SZ_256];
            rg_folder_key(b->file_paths[f], key, sizeof(key));
            void *slot = cbm_ht_get(folder_ht, key);
            int fi;
            if (!slot) {
                if (folder_count >= folder_cap) {
                    int nc = folder_cap ? folder_cap * 2 : 64;
                    char **nk =
                        cbm_realloc(CBM_MEM_CLASS_OTHER, folder_keys, (size_t)nc * sizeof(char *));
                    int *nf =
                        cbm_realloc(CBM_MEM_CLASS_OTHER, folder_files, (size_t)nc * sizeof(int));
                    if (!nk || !nf) {
                        cbm_free(CBM_MEM_CLASS_OTHER, nk ? nk : folder_keys);
                        cbm_free(CBM_MEM_CLASS_OTHER, nf ? nf : folder_files);
                        folder_keys = NULL;
                        folder_files = NULL;
                        folder_count = 0;
                        break;
                    }
                    folder_keys = nk;
                    folder_files = nf;
                    folder_cap = nc;
                }
                folder_keys[folder_count] = rg_strdup(key);
                folder_files[folder_count] = 0;
                cbm_ht_set(folder_ht, folder_keys[folder_count],
                           (void *)(intptr_t)(folder_count + 1));
                fi = folder_count;
                folder_count++;
            } else {
                fi = (int)(intptr_t)slot - 1;
            }
            folder_files[fi]++;
        }
    }
    if (folder_count > 0) {
        rg_rank_t *rank = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)folder_count * sizeof(rg_rank_t));
        if (rank) {
            for (int i = 0; i < folder_count; i++)
                rank[i] = (rg_rank_t){i, folder_files[i]};
            qsort(rank, (size_t)folder_count, sizeof(rg_rank_t), rg_rank_cmp);
            /* folder slot → region id, only for kept groups */
            int *folder_region = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)folder_count * sizeof(int));
            if (folder_region) {
                for (int i = 0; i < folder_count; i++)
                    folder_region[i] = -1;
                for (int i = 0; i < folder_count && region_count < REGION_MAX_TOTAL - 1; i++) {
                    if (rank[i].files < REGION_FOLDER_MIN_FILES)
                        break;
                    folder_region[rank[i].key] = region_count;
                    regions[region_count].files = rank[i].files;
                    regions[region_count].name = rg_strdup(folder_keys[rank[i].key]);
                    regions[region_count].why = rg_strdup("folder group (not explained by a kept "
                                                          "call community)");
                    region_count++;
                }
                for (int f = 0; f < b->file_count; f++) {
                    if (b->file_recs[f]->region >= 0)
                        continue;
                    char key[CBM_SZ_256];
                    rg_folder_key(b->file_paths[f], key, sizeof(key));
                    void *slot = cbm_ht_get(folder_ht, key);
                    if (slot) {
                        int fr = folder_region[(int)(intptr_t)slot - 1];
                        if (fr >= 0)
                            b->file_recs[f]->region = fr;
                    }
                }
                cbm_free(CBM_MEM_CLASS_OTHER, folder_region);
            }
            cbm_free(CBM_MEM_CLASS_OTHER, rank);
        }
    }
    for (int i = 0; i < folder_count; i++)
        cbm_free(CBM_MEM_CLASS_OTHER, folder_keys[i]);
    cbm_free(CBM_MEM_CLASS_OTHER, folder_keys);
    cbm_free(CBM_MEM_CLASS_OTHER, folder_files);
    cbm_ht_free(folder_ht);

    /* 5. Misc region for whatever is left (created lazily in the node pass
     * for files first seen there, too). */
    int misc_region = -1;
    for (int f = 0; f < b->file_count; f++) {
        if (b->file_recs[f]->region >= 0)
            continue;
        if (misc_region < 0 && region_count < REGION_MAX_TOTAL) {
            misc_region = region_count;
            regions[misc_region].name = rg_strdup("misc");
            regions[misc_region].why = rg_strdup("files outside every kept community and folder "
                                                 "group");
            region_count++;
        }
        if (misc_region >= 0) {
            b->file_recs[f]->region = misc_region;
            regions[misc_region].files++;
        }
    }

    /* 6. Names for the Leiden regions: dominant folder key (majority of
     * member files), with the key strings owned by a local list. */
    for (int r = 0; r < leiden_regions; r++) {
        CBMHashTable *dir_ht = cbm_ht_create(64); /* key → count (borrowed keys) */
        char **dir_keys = NULL;
        int dir_count = 0, dir_cap = 0;
        char *best_key = NULL;
        int best = 0;
        if (dir_ht) {
            for (int f = 0; f < b->file_count; f++) {
                if (b->file_recs[f]->region != r)
                    continue;
                char key[CBM_SZ_256];
                rg_folder_key(b->file_paths[f], key, sizeof(key));
                const char *canon = cbm_ht_get_key(dir_ht, key);
                if (!canon) {
                    if (dir_count >= dir_cap) {
                        int nc = dir_cap ? dir_cap * 2 : 32;
                        char **nk =
                            cbm_realloc(CBM_MEM_CLASS_OTHER, dir_keys, (size_t)nc * sizeof(char *));
                        if (!nk)
                            break;
                        dir_keys = nk;
                        dir_cap = nc;
                    }
                    dir_keys[dir_count] = rg_strdup(key);
                    if (!dir_keys[dir_count])
                        break;
                    canon = dir_keys[dir_count];
                    dir_count++;
                }
                int cnt = (int)(intptr_t)cbm_ht_get(dir_ht, canon) + 1;
                cbm_ht_set(dir_ht, canon, (void *)(intptr_t)cnt);
                if (cnt > best) {
                    best = cnt;
                    cbm_free(CBM_MEM_CLASS_OTHER, best_key);
                    best_key = rg_strdup(canon);
                }
            }
        }
        regions[r].name = best_key ? best_key : rg_strdup("(cross-cutting)");
        char why[CBM_SZ_512];
        snprintf(why, sizeof(why), "call community: %d files, %d%% under %s", regions[r].files,
                 regions[r].files > 0 ? best * 100 / regions[r].files : 0,
                 regions[r].name ? regions[r].name : "?");
        regions[r].why = rg_strdup(why);
        for (int d = 0; d < dir_count; d++)
            cbm_free(CBM_MEM_CLASS_OTHER, dir_keys[d]);
        cbm_free(CBM_MEM_CLASS_OTHER, dir_keys);
        cbm_ht_free(dir_ht);
    }

    /* Top-k per region: the k highest-degree callables (name + hub). */
    {
        int k = REGION_TOP_NODES;
        int *top_deg =
            cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)region_count * (size_t)k * sizeof(int));
        int *top_idx =
            cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)region_count * (size_t)k * sizeof(int));
        if (top_deg && top_idx) {
            for (int i = 0; i < region_count * k; i++) {
                top_deg[i] = -1;
                top_idx[i] = -1;
            }
            for (int i = 0; i < cn; i++) {
                int r = b->file_recs[cfile[i]]->region;
                if (r < 0 || r >= region_count)
                    continue;
                int d = cdeg ? cdeg[i] : 0;
                int base = r * k;
                for (int t = 0; t < k; t++) {
                    if (d > top_deg[base + t]) {
                        for (int u = k - 1; u > t; u--) {
                            top_deg[base + u] = top_deg[base + u - 1];
                            top_idx[base + u] = top_idx[base + u - 1];
                        }
                        top_deg[base + t] = d;
                        top_idx[base + t] = i;
                        break;
                    }
                }
            }
            for (int r = 0; r < region_count; r++) {
                for (int t = 0; t < k; t++) {
                    int i = top_idx[r * k + t];
                    if (i < 0 || !cnames[i])
                        continue;
                    regions[r].top_nodes[regions[r].top_count++] = rg_strdup(cnames[i]);
                }
                if (regions[r].top_count > 0 && !regions[r].hub)
                    regions[r].hub = rg_strdup(regions[r].top_nodes[0]);
            }
        }
        cbm_free(CBM_MEM_CLASS_OTHER, top_deg);
        cbm_free(CBM_MEM_CLASS_OTHER, top_idx);
    }

    for (int i = 0; i < cn; i++)
        cbm_free(CBM_MEM_CLASS_OTHER, cnames[i]);
    cbm_free(CBM_MEM_CLASS_OTHER, cnames);
    cbm_free(CBM_MEM_CLASS_OTHER, cids);
    cbm_free(CBM_MEM_CLASS_OTHER, cfile);
    cbm_free(CBM_MEM_CLASS_OTHER, cdeg);
    cbm_free(CBM_MEM_CLASS_OTHER, ccomm);
    cbm_free(CBM_MEM_CLASS_OTHER, comm_files);
    cbm_free(CBM_MEM_CLASS_OTHER, comm_region);

    b->regions = regions;
    b->region_count = region_count;
    b->method = use_leiden ? "leiden+folders" : "folders";
    return 0;
}

/* Full node pass: map every node to a region via its file; count members.
 * Files not seen in the callable pass join folder/misc regions here. */
static int rg_map_all_nodes(cbm_store_t *store, const char *project, CBMHashTable *file_ht,
                            rg_build_t *b) {
    struct sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *st = NULL;
    if (!db || sqlite3_prepare_v2(db, "SELECT id, file_path FROM nodes WHERE project=?1", -1, &st,
                                  NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);

    /* folder key → region id for late files (folder regions + misc). */
    CBMHashTable *late_ht = cbm_ht_create(128);
    for (int r = 0; r < b->region_count; r++) {
        /* Folder regions are addressable by their name (the key). Leiden
         * region names may collide with folder keys; folder regions were
         * created later, so set them last — but only map names that ARE
         * folder keys (why starts with "folder group"). */
        if (b->regions[r].why && strncmp(b->regions[r].why, "folder group", 12) == 0)
            cbm_ht_set(late_ht, b->regions[r].name, (void *)(intptr_t)(r + 1));
    }
    int misc_region = -1;
    for (int r = 0; r < b->region_count; r++)
        if (b->regions[r].name && strcmp(b->regions[r].name, "misc") == 0)
            misc_region = r;

    long long cap = 0, n = 0;
    rg_id_region_t *map = NULL;
    int file_cap_local = b->file_count; /* interning may grow the arrays */
    while (sqlite3_step(st) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(st, 0);
        const char *path = (const char *)sqlite3_column_text(st, 1);
        int region = -1;
        if (path && path[0]) {
            file_rec_t *rec = cbm_ht_get(file_ht, path);
            if (!rec) {
                rec = rg_file_intern(file_ht, b, &file_cap_local, path);
                if (rec) {
                    char key[CBM_SZ_256];
                    rg_folder_key(path, key, sizeof(key));
                    void *slot = cbm_ht_get(late_ht, key);
                    if (slot)
                        rec->region = (int)(intptr_t)slot - 1;
                    else if (misc_region >= 0)
                        rec->region = misc_region;
                    if (rec->region >= 0)
                        b->regions[rec->region].files++;
                }
            }
            if (rec)
                region = rec->region;
        }
        if (n >= cap) {
            long long nc = cap ? cap * 2 : 65536;
            rg_id_region_t *nm =
                cbm_realloc(CBM_MEM_CLASS_OTHER, map, (size_t)nc * sizeof(rg_id_region_t));
            if (!nm)
                break;
            map = nm;
            cap = nc;
        }
        map[n].id = id;
        map[n].region = region;
        n++;
        if (region >= 0)
            b->regions[region].members++;
        else
            b->unmapped_nodes++;
    }
    sqlite3_finalize(st);
    cbm_ht_free(late_ht);
    if (map)
        qsort(map, (size_t)n, sizeof(rg_id_region_t), rg_id_region_cmp);
    b->node_map = map;
    b->node_map_count = n;
    b->total_nodes = n;
    return map || n == 0 ? 0 : -1;
}

/* Edge pass: aggregate weights between regions across ALL edge types. */
static int rg_aggregate_edges(cbm_store_t *store, const char *project, rg_build_t *b) {
    int rc2 = b->region_count * b->region_count;
    b->edge_w = cbm_atlas_calloc_array(rc2 > 0 ? (size_t)rc2 : 1, sizeof(long long));
    if (!b->edge_w)
        return -1;
    struct sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *st = NULL;
    if (!db || sqlite3_prepare_v2(db, "SELECT source_id, target_id FROM edges WHERE project=?1", -1,
                                  &st, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        int rs = rg_region_of(b->node_map, b->node_map_count, sqlite3_column_int64(st, 0));
        int rt = rg_region_of(b->node_map, b->node_map_count, sqlite3_column_int64(st, 1));
        if (rs < 0 || rt < 0)
            continue;
        if (rs == rt) {
            b->regions[rs].internal_edges++;
        } else {
            b->regions[rs].boundary_edges++;
            b->regions[rt].boundary_edges++;
            b->edge_w[rs * b->region_count + rt]++;
        }
    }
    sqlite3_finalize(st);
    return 0;
}

/* Layout: golden-angle sphere seed + the shared optimizer, edges replicated
 * by log2(weight) so heavy couplings pull harder. Deterministic. */
static void rg_layout(rg_build_t *b, float *out_xyz, float *out_size) {
    int rc = b->region_count;
    cbm_layout_body_t *bodies =
        cbm_atlas_calloc_array(rc > 0 ? (size_t)rc : 1, sizeof(cbm_layout_body_t));
    if (!bodies)
        return;
    long long max_members = 1;
    for (int r = 0; r < rc; r++)
        if (b->regions[r].members > max_members)
            max_members = b->regions[r].members;
    const float golden = 2.39996323f; /* golden angle in radians */
    for (int r = 0; r < rc; r++) {
        float t = rc > 1 ? (float)r / (float)(rc - 1) : 0.5f;
        float inclination = acosf(1.0f - 2.0f * t);
        float azimuth = golden * (float)r;
        float radius = (float)REGION_SEED_RADIUS;
        bodies[r].x = radius * sinf(inclination) * cosf(azimuth);
        bodies[r].y = radius * sinf(inclination) * sinf(azimuth);
        bodies[r].z = radius * cosf(inclination);
        bodies[r].ax = bodies[r].x;
        bodies[r].ay = bodies[r].y;
        bodies[r].az = bodies[r].z;
        bodies[r].mass = (float)(1.0 + sqrt((double)b->regions[r].members));
    }
    /* Replicated edge list. */
    int ecap = 0, ne = 0;
    int *es = NULL, *ed = NULL;
    for (int s = 0; s < rc; s++) {
        for (int t2 = 0; t2 < rc; t2++) {
            long long w = b->edge_w[s * rc + t2];
            if (w <= 0)
                continue;
            int rep = 1;
            while ((1LL << rep) < w && rep < REGION_EDGE_PULL_CAP)
                rep++;
            for (int k = 0; k < rep; k++) {
                if (ne >= ecap) {
                    int nc = ecap ? ecap * 2 : 256;
                    int *ns = cbm_realloc(CBM_MEM_CLASS_OTHER, es, (size_t)nc * sizeof(int));
                    int *nd = cbm_realloc(CBM_MEM_CLASS_OTHER, ed, (size_t)nc * sizeof(int));
                    if (!ns || !nd) {
                        cbm_free(CBM_MEM_CLASS_OTHER, ns ? ns : es);
                        cbm_free(CBM_MEM_CLASS_OTHER, nd ? nd : ed);
                        es = NULL;
                        ed = NULL;
                        ne = 0;
                        goto layout;
                    }
                    es = ns;
                    ed = nd;
                    ecap = nc;
                }
                es[ne] = s;
                ed[ne] = t2;
                ne++;
            }
        }
    }
layout:
    if (es && ed)
        cbm_layout_local_optimize(bodies, rc, es, ed, ne);
    for (int r = 0; r < rc; r++) {
        out_xyz[r * 3 + 0] = bodies[r].x;
        out_xyz[r * 3 + 1] = bodies[r].y;
        out_xyz[r * 3 + 2] = bodies[r].z;
        double frac = max_members > 0 ? (double)b->regions[r].members / (double)max_members : 0.0;
        out_size[r] = 14.0f + (float)(sqrt(frac) * 46.0);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, es);
    cbm_free(CBM_MEM_CLASS_OTHER, ed);
    cbm_free(CBM_MEM_CLASS_OTHER, bodies);
}

/* Serialize the build to the level=regions payload. */
static char *rg_to_json(const rg_build_t *b, const float *xyz, const float *size) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc)
        return NULL;
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "level", "regions");
    yyjson_mut_obj_add_str(doc, root, "method", b->method);
    yyjson_mut_obj_add_int(doc, root, "total_nodes", b->total_nodes);
    yyjson_mut_obj_add_int(doc, root, "unmapped_nodes", b->unmapped_nodes);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int r = 0; r < b->region_count; r++) {
        const region_t *rg = &b->regions[r];
        yyjson_mut_val *o = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, o, "id", r);
        yyjson_mut_obj_add_strcpy(doc, o, "name", rg->name ? rg->name : "?");
        if (rg->hub)
            yyjson_mut_obj_add_strcpy(doc, o, "hub", rg->hub);
        if (rg->why)
            yyjson_mut_obj_add_strcpy(doc, o, "why", rg->why);
        yyjson_mut_obj_add_int(doc, o, "files", rg->files);
        yyjson_mut_obj_add_int(doc, o, "members", rg->members);
        double denom = (double)rg->internal_edges + (double)rg->boundary_edges;
        yyjson_mut_obj_add_real(doc, o, "cohesion",
                                denom > 0 ? (double)rg->internal_edges / denom : 0.0);
        yyjson_mut_val *tops = yyjson_mut_arr(doc);
        for (int t = 0; t < rg->top_count; t++)
            yyjson_mut_arr_add_strcpy(doc, tops, rg->top_nodes[t]);
        yyjson_mut_obj_add_val(doc, o, "top_nodes", tops);
        yyjson_mut_obj_add_real(doc, o, "x", xyz[r * 3 + 0]);
        yyjson_mut_obj_add_real(doc, o, "y", xyz[r * 3 + 1]);
        yyjson_mut_obj_add_real(doc, o, "z", xyz[r * 3 + 2]);
        yyjson_mut_obj_add_real(doc, o, "size", size[r]);
        char color[8];
        snprintf(color, sizeof(color), "#%06x", rg_wheel_color(r, b->region_count));
        yyjson_mut_obj_add_strcpy(doc, o, "color", color);
        yyjson_mut_arr_append(arr, o);
    }
    yyjson_mut_obj_add_val(doc, root, "regions", arr);
    yyjson_mut_val *earr = yyjson_mut_arr(doc);
    for (int s = 0; s < b->region_count; s++) {
        for (int t = 0; t < b->region_count; t++) {
            long long w = b->edge_w[s * b->region_count + t];
            if (w <= 0)
                continue;
            yyjson_mut_val *e = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, e, "source", s);
            yyjson_mut_obj_add_int(doc, e, "target", t);
            yyjson_mut_obj_add_int(doc, e, "weight", w);
            yyjson_mut_arr_append(earr, e);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "edges", earr);
    char *json = yyjson_mut_write_opts(doc, 0, cbm_json_writer_allocator(), NULL, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Build everything and refresh the cache. Caller holds g_region_mu. */
static int rg_rebuild_locked(cbm_store_t *store, const char *project, const char *key) {
    rg_build_t b;
    memset(&b, 0, sizeof(b));
    CBMHashTable *file_ht = cbm_ht_create(4096);
    if (!file_ht)
        return -1;
    int rc = rg_assign_regions(store, project, file_ht, &b);
    if (rc == 0)
        rc = rg_map_all_nodes(store, project, file_ht, &b);
    if (rc == 0)
        rc = rg_aggregate_edges(store, project, &b);
    char *json = NULL;
    if (rc == 0) {
        float *xyz = cbm_atlas_calloc_array(b.region_count > 0 ? (size_t)b.region_count * 3 : 1,
                                            sizeof(float));
        float *size =
            cbm_atlas_calloc_array(b.region_count > 0 ? (size_t)b.region_count : 1, sizeof(float));
        if (xyz && size) {
            rg_layout(&b, xyz, size);
            json = rg_to_json(&b, xyz, size);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, xyz);
        cbm_free(CBM_MEM_CLASS_OTHER, size);
    }
    if (!json) {
        rg_build_free(&b);
        cbm_ht_free(file_ht);
        return -1;
    }

    /* Refresh the cache: JSON + per-region file lists for scope queries. */
    region_cache_clear_locked();
    snprintf(g_region_cache.key, sizeof(g_region_cache.key), "%s", key);
    g_region_cache.json = json;
    g_region_cache.region_count = b.region_count;
    g_region_cache.total_nodes = b.total_nodes;
    g_region_cache.region_files =
        cbm_atlas_calloc_array(b.region_count > 0 ? (size_t)b.region_count : 1, sizeof(char **));
    g_region_cache.region_file_count =
        cbm_atlas_calloc_array(b.region_count > 0 ? (size_t)b.region_count : 1, sizeof(int));
    g_region_cache.file_ht = cbm_ht_create((uint32_t)(b.file_count > 0 ? b.file_count : 16));
    if (g_region_cache.region_files && g_region_cache.region_file_count) {
        for (int f = 0; f < b.file_count; f++) {
            int r = b.file_recs[f]->region;
            if (r < 0 || r >= b.region_count)
                continue;
            int idx = g_region_cache.region_file_count[r];
            char **grown = cbm_realloc(CBM_MEM_CLASS_OTHER, g_region_cache.region_files[r],
                                       ((size_t)idx + 1) * sizeof(char *));
            if (!grown)
                continue;
            g_region_cache.region_files[r] = grown;
            g_region_cache.region_files[r][idx] = rg_strdup(b.file_paths[f]);
            if (g_region_cache.region_files[r][idx]) {
                g_region_cache.region_file_count[r] = idx + 1;
                if (g_region_cache.file_ht)
                    cbm_ht_set(g_region_cache.file_ht, g_region_cache.region_files[r][idx],
                               (void *)(intptr_t)(r + 1));
            }
        }
    }
    rg_build_free(&b);
    cbm_ht_free(file_ht);
    return 0;
}

static bool rg_cache_fresh_locked(const char *key) {
    return g_region_cache.json && strcmp(g_region_cache.key, key) == 0;
}

char *cbm_layout_regions_json(cbm_store_t *store, const char *project) {
    if (!store || !project)
        return NULL;
    char key[1152];
    region_cache_key(store, project, key, sizeof(key));
    pthread_mutex_lock(&g_region_mu);
    if (!rg_cache_fresh_locked(key)) {
        if (rg_rebuild_locked(store, project, key) != 0) {
            pthread_mutex_unlock(&g_region_mu);
            return NULL;
        }
    }
    char *copy = rg_strdup(g_region_cache.json);
    pthread_mutex_unlock(&g_region_mu);
    return copy;
}

/* ── Region scope: full-detail layout of one region's nodes ───── */

enum { RG_SCOPE_IN_CHUNK = 200 };

cbm_layout_result_t *cbm_layout_compute_region(cbm_store_t *store, const char *project,
                                               int region_id, int max_nodes) {
    if (!store || !project || region_id < 0)
        return NULL;
    char key[1152];
    region_cache_key(store, project, key, sizeof(key));
    pthread_mutex_lock(&g_region_mu);
    if (!rg_cache_fresh_locked(key) && rg_rebuild_locked(store, project, key) != 0) {
        pthread_mutex_unlock(&g_region_mu);
        return NULL;
    }
    if (region_id >= g_region_cache.region_count) {
        pthread_mutex_unlock(&g_region_mu);
        return NULL;
    }
    /* Copy the file list out so the store queries run unlocked. */
    int file_count = g_region_cache.region_file_count[region_id];
    char **files = cbm_atlas_calloc_array(file_count > 0 ? (size_t)file_count : 1, sizeof(char *));
    if (!files) {
        pthread_mutex_unlock(&g_region_mu);
        return NULL;
    }
    for (int f = 0; f < file_count; f++)
        files[f] = rg_strdup(g_region_cache.region_files[region_id][f]);
    pthread_mutex_unlock(&g_region_mu);

    if (max_nodes <= 0)
        max_nodes = 5000;

    /* Fetch nodes file-chunk by file-chunk. */
    struct sqlite3 *db = cbm_store_get_db(store);
    cbm_node_t *nodes = NULL;
    int n = 0, cap = 0;
    long long region_total = 0;
    if (db) {
        for (int off = 0; off < file_count; off += RG_SCOPE_IN_CHUNK) {
            int cnt = file_count - off < RG_SCOPE_IN_CHUNK ? file_count - off : RG_SCOPE_IN_CHUNK;
            char sql[8192];
            int pos = snprintf(sql, sizeof(sql),
                               "SELECT id, label, name, qualified_name, file_path, start_line, "
                               "end_line, properties FROM nodes WHERE project=?1 AND file_path "
                               "IN (");
            for (int i = 0; i < cnt && pos < (int)sizeof(sql) - 8; i++)
                pos += snprintf(sql + pos, sizeof(sql) - (size_t)pos, "%s?%d", i ? "," : "", i + 2);
            snprintf(sql + pos, sizeof(sql) - (size_t)pos, ") ORDER BY id");
            sqlite3_stmt *st = NULL;
            if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK)
                break;
            sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
            for (int i = 0; i < cnt; i++)
                sqlite3_bind_text(st, i + 2, files[off + i], -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                region_total++;
                if (n >= max_nodes)
                    continue; /* keep counting the region total */
                if (n >= cap) {
                    int nc = cap ? cap * 2 : 1024;
                    cbm_node_t *nn =
                        cbm_realloc(CBM_MEM_CLASS_OTHER, nodes, (size_t)nc * sizeof(cbm_node_t));
                    if (!nn)
                        break;
                    nodes = nn;
                    cap = nc;
                }
                memset(&nodes[n], 0, sizeof(cbm_node_t));
                nodes[n].id = sqlite3_column_int64(st, 0);
                nodes[n].project = project;
                nodes[n].label = rg_strdup((const char *)sqlite3_column_text(st, 1));
                nodes[n].name = rg_strdup((const char *)sqlite3_column_text(st, 2));
                nodes[n].qualified_name = rg_strdup((const char *)sqlite3_column_text(st, 3));
                nodes[n].file_path = rg_strdup((const char *)sqlite3_column_text(st, 4));
                nodes[n].start_line = sqlite3_column_int(st, 5);
                nodes[n].end_line = sqlite3_column_int(st, 6);
                nodes[n].properties_json = rg_strdup((const char *)sqlite3_column_text(st, 7));
                n++;
            }
            sqlite3_finalize(st);
        }
    }
    for (int f = 0; f < file_count; f++)
        cbm_free(CBM_MEM_CLASS_OTHER, files[f]);
    cbm_free(CBM_MEM_CLASS_OTHER, files);

    cbm_layout_result_t *result =
        cbm_layout_from_nodes(store, project, nodes, n, (int)region_total);
    for (int i = 0; i < n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)nodes[i].label);
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)nodes[i].name);
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)nodes[i].qualified_name);
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)nodes[i].file_path);
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)nodes[i].properties_json);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, nodes);
    return result;
}

/* ── Cache-backed region lookup for other Atlas services ──────── */

int cbm_layout_region_for_file(cbm_store_t *store, const char *project, const char *file_path,
                               char **name_out) {
    if (name_out)
        *name_out = NULL;
    if (!store || !project || !file_path || !file_path[0])
        return -1;
    char key[1152];
    region_cache_key(store, project, key, sizeof(key));
    pthread_mutex_lock(&g_region_mu);
    if (!rg_cache_fresh_locked(key) && rg_rebuild_locked(store, project, key) != 0) {
        pthread_mutex_unlock(&g_region_mu);
        return -1;
    }
    int found = -1;
    if (g_region_cache.file_ht) {
        void *slot = cbm_ht_get(g_region_cache.file_ht, file_path);
        if (slot)
            found = (int)(intptr_t)slot - 1;
    }
    if (found >= 0 && name_out && g_region_cache.json) {
        /* Region names live in the serialized payload; re-parse the one we
         * need rather than duplicating the name store. */
        yyjson_doc *doc = yyjson_read(g_region_cache.json, strlen(g_region_cache.json), 0);
        if (doc) {
            yyjson_val *regions = yyjson_obj_get(yyjson_doc_get_root(doc), "regions");
            yyjson_val *rv = yyjson_arr_get(regions, (size_t)found);
            const char *name = yyjson_get_str(yyjson_obj_get(rv, "name"));
            if (name)
                *name_out = rg_strdup(name);
            yyjson_doc_free(doc);
        }
    }
    pthread_mutex_unlock(&g_region_mu);
    return found;
}
