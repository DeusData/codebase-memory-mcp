/*
 * result_spill.c — see result_spill.h.
 *
 * Layout on disk, per parked result:
 *   spill_rec_hdr_t  (magic, block length, block base address at park time,
 *                     the CBMFileResult header as it was in memory)
 *   block bytes      (the single compacted arena block)
 *
 * The header's pointers are meaningless on disk; the loader rebuilds the
 * arena at a new address and shifts every pointer by the delta
 * (cbm_result_relocate, implemented on the compaction traversal so it sees
 * exactly the fields compaction copied).
 */

#include "result_spill.h"

#include "foundation/arena.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"

/* The park writes a result as an opaque image: the record header (a struct
 * copy) and the compacted arena block. Both carry padding bytes no code ever
 * wrote -- inside structs, between objects -- and MemorySanitizer tracks
 * that mark through the compaction's memcpy, so it refuses the fwrite of an
 * image that is read back whole and never interpreted byte by byte (CI MSan
 * lane on #2202: offset 4087, then 6714, of a 6,952-byte block). Under MSan
 * the image is declared defined right before the write; every other build
 * compiles this to nothing. */
#include "foundation/sanitized.h" /* __has_feature exists everywhere, cppcheck included */
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#define SPILL_IMAGE_DEFINED(p, n) __msan_unpoison((p), (n))
#else
#define SPILL_IMAGE_DEFINED(p, n) ((void)0)
#endif

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <process.h>
#define SPILL_PID() ((long)_getpid())
#define SPILL_SEEK(fp, off) _fseeki64((fp), (long long)(off), SEEK_SET)
#else
#include <unistd.h>
#define SPILL_PID() ((long)getpid())
#define SPILL_SEEK(fp, off) fseeko((fp), (off_t)(off), SEEK_SET)
#endif

static const uint64_t SPILL_MAGIC = 0x5350494C4C524553ULL; /* "SPILLRES" */

typedef struct {
    uint64_t magic;
    uint64_t block_len;
    uint64_t old_base; /* (uintptr_t) of the block when parked */
    CBMFileResult header;
} spill_rec_hdr_t;

typedef struct {
    FILE *fp;
    cbm_mutex_t mu; /* reads seek; writes append -- one lock per file */
    char path[CBM_SZ_1K];
    uint64_t end; /* bytes written so far (append offset) */
} spill_file_t;

typedef struct {
    int writer;       /* -1 = empty */
    uint64_t offset;  /* record start in that writer's file */
    uint64_t rec_len; /* header + block */
} spill_slot_t;

struct cbm_result_spill {
    spill_file_t *files;
    int writers;
    spill_slot_t *slots;
    int slot_count;
    _Atomic int64_t parked;
    _Atomic int64_t bytes;
    _Atomic int64_t loads;
};

cbm_result_spill_t *cbm_result_spill_open(const char *dir, int writers, int slots) {
    if (!dir || !dir[0] || writers <= 0 || slots <= 0) {
        return NULL;
    }
    char spill_dir[CBM_SZ_1K];
    if (snprintf(spill_dir, sizeof(spill_dir), "%s/spill", dir) >= (int)sizeof(spill_dir)) {
        return NULL;
    }
    if (!cbm_mkdir_p(spill_dir, 0700)) {
        cbm_log_warn("mem.spill.open_failed", "dir", spill_dir, "reason", "mkdir");
        return NULL;
    }
    cbm_result_spill_t *sp = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*sp));
    if (!sp) {
        return NULL;
    }
    sp->files = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)writers * sizeof(spill_file_t));
    sp->slots = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)slots * sizeof(spill_slot_t));
    if (!sp->files || !sp->slots) {
        cbm_result_spill_close(sp);
        return NULL;
    }
    sp->writers = writers;
    sp->slot_count = slots;
    for (int i = 0; i < slots; i++) {
        sp->slots[i].writer = -1;
    }
    for (int w = 0; w < writers; w++) {
        spill_file_t *f = &sp->files[w];
        snprintf(f->path, sizeof(f->path), "%s/spill-%ld-%d.bin", spill_dir, SPILL_PID(), w);
        f->fp = cbm_fopen(f->path, "w+b");
        if (!f->fp) {
            cbm_log_warn("mem.spill.open_failed", "path", f->path, "reason", "fopen");
            cbm_result_spill_close(sp);
            return NULL;
        }
        cbm_mutex_init(&f->mu);
    }
    cbm_log_info("mem.spill.open", "dir", spill_dir, "writers", writers > 0 ? "yes" : "no");
    return sp;
}

bool cbm_result_spill_has(const cbm_result_spill_t *sp, int slot) {
    return sp && slot >= 0 && slot < sp->slot_count && sp->slots[slot].writer >= 0;
}

bool cbm_result_spill_park(cbm_result_spill_t *sp, int writer, int slot, CBMFileResult *result) {
    if (!sp || !result || writer < 0 || writer >= sp->writers || slot < 0 ||
        slot >= sp->slot_count || sp->slots[slot].writer >= 0) {
        return false;
    }
    /* Only a compacted result is one block with every pointer inside it; a
     * result that owns sub-results (embedded languages) has more arenas than
     * that and stays in memory. A retained parse tree is a re-parse cache,
     * not data: it is dropped with the in-memory result below. */
    if (result->arena.nblocks != 1 || result->owned_result_count != 0) {
        return false;
    }
    spill_file_t *f = &sp->files[writer];
    spill_rec_hdr_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = SPILL_MAGIC;
    hdr.block_len = result->arena.used; /* bytes actually written into the block */
    hdr.old_base = (uint64_t)(uintptr_t)result->arena.blocks[0];
    hdr.header = *result;
    hdr.header.cached_tree = NULL; /* never on disk: the loader gets no tree */
    SPILL_IMAGE_DEFINED(&hdr, sizeof(hdr));
    SPILL_IMAGE_DEFINED(result->arena.blocks[0], hdr.block_len);
    cbm_mutex_lock(&f->mu);
    uint64_t offset = f->end;
    bool ok = SPILL_SEEK(f->fp, offset) == 0 && fwrite(&hdr, sizeof(hdr), 1, f->fp) == 1 &&
              (hdr.block_len == 0 || fwrite(result->arena.blocks[0], hdr.block_len, 1, f->fp) == 1);
    if (ok) {
        f->end = offset + sizeof(hdr) + hdr.block_len;
    }
    cbm_mutex_unlock(&f->mu);
    if (!ok) {
        cbm_log_warn("mem.spill.write_failed", "path", f->path);
        return false;
    }
    sp->slots[slot].writer = writer;
    sp->slots[slot].offset = offset;
    sp->slots[slot].rec_len = sizeof(hdr) + hdr.block_len;
    atomic_fetch_add_explicit(&sp->parked, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&sp->bytes, (int64_t)(sizeof(hdr) + hdr.block_len),
                              memory_order_relaxed);
    cbm_free_result(result);
    return true;
}

CBMFileResult *cbm_result_spill_load(const cbm_result_spill_t *sp, int slot) {
    if (!cbm_result_spill_has(sp, slot)) {
        return NULL;
    }
    const spill_slot_t *s = &sp->slots[slot];
    spill_file_t *f = &sp->files[s->writer];
    spill_rec_hdr_t hdr;
    cbm_mutex_lock(&f->mu);
    bool ok = SPILL_SEEK(f->fp, s->offset) == 0 && fread(&hdr, sizeof(hdr), 1, f->fp) == 1 &&
              hdr.magic == SPILL_MAGIC;
    CBMFileResult *r = NULL;
    if (ok) {
        r = cbm_result_alloc();
        if (r) {
            *r = hdr.header;
            r->cached_tree = NULL;
            memset(&r->arena, 0, sizeof(r->arena));
            cbm_arena_init_exact(&r->arena, hdr.block_len ? (size_t)hdr.block_len : 1);
            if (r->arena.nblocks == 1 &&
                (hdr.block_len == 0 ||
                 fread(r->arena.blocks[0], (size_t)hdr.block_len, 1, f->fp) == 1)) {
                r->arena.used = (size_t)hdr.block_len;
                r->arena.total_alloc = (size_t)hdr.block_len;
            } else {
                ok = false;
            }
        } else {
            ok = false;
        }
    }
    cbm_mutex_unlock(&f->mu);
    if (!ok) {
        cbm_log_warn("mem.spill.read_failed", "path", f->path);
        if (r) {
            cbm_free_result(r);
        }
        return NULL;
    }
    cbm_result_relocate(r, (const char *)(uintptr_t)hdr.old_base, (size_t)hdr.block_len,
                        r->arena.blocks[0]);
    atomic_fetch_add_explicit((_Atomic int64_t *)&sp->loads, 1, memory_order_relaxed);
    return r;
}

bool cbm_result_spill_peek_header(const cbm_result_spill_t *sp, int slot, CBMFileResult *out) {
    if (!out || !cbm_result_spill_has(sp, slot)) {
        return false;
    }
    const spill_slot_t *s = &sp->slots[slot];
    spill_file_t *f = &sp->files[s->writer];
    spill_rec_hdr_t hdr;
    cbm_mutex_lock(&f->mu);
    bool ok = SPILL_SEEK(f->fp, s->offset) == 0 && fread(&hdr, sizeof(hdr), 1, f->fp) == 1 &&
              hdr.magic == SPILL_MAGIC;
    cbm_mutex_unlock(&f->mu);
    if (ok) {
        *out = hdr.header;
    }
    return ok;
}

void cbm_result_spill_peek_counts(const cbm_result_spill_t *sp, int slot, int *defs, int *impls) {
    CBMFileResult hdr;
    bool ok = cbm_result_spill_peek_header(sp, slot, &hdr);
    if (defs) {
        *defs = ok ? hdr.defs.count : 0;
    }
    if (impls) {
        *impls = ok ? hdr.impl_traits.count : 0;
    }
}

void cbm_result_spill_stats(const cbm_result_spill_t *sp, int64_t *parked, int64_t *bytes,
                            int64_t *loads) {
    if (parked) {
        *parked = sp ? atomic_load_explicit(&sp->parked, memory_order_relaxed) : 0;
    }
    if (bytes) {
        *bytes = sp ? atomic_load_explicit(&sp->bytes, memory_order_relaxed) : 0;
    }
    if (loads) {
        *loads = sp ? atomic_load_explicit(&sp->loads, memory_order_relaxed) : 0;
    }
}

void cbm_result_spill_close(cbm_result_spill_t *sp) {
    if (!sp) {
        return;
    }
    if (sp->files) {
        for (int w = 0; w < sp->writers; w++) {
            spill_file_t *f = &sp->files[w];
            if (f->fp) {
                (void)fclose(f->fp);
                (void)cbm_unlink(f->path);
                cbm_mutex_destroy(&f->mu);
            }
        }
        cbm_free(CBM_MEM_CLASS_OTHER, sp->files);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, sp->slots);
    cbm_free(CBM_MEM_CLASS_OTHER, sp);
}
