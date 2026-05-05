// sqlite_writer.c — Direct SQLite page writer.
// Constructs a valid .db file from sorted in-memory data without using
// the SQL parser, INSERT statements, or B-tree rebalancing.
//
// SQLite file format reference: https://www.sqlite.org/fileformat2.html
//
// Key invariants:
//   - Page size: 4096 bytes
//   - Page 1 has a 100-byte database header before the B-tree header
//   - Leaf table B-tree pages: flag 0x0D
//   - Interior table B-tree pages: flag 0x05
//   - Leaf index B-tree pages: flag 0x0A
//   - Interior index B-tree pages: flag 0x02
//   - Records: header (varint count + serial types) + body (column values)
//   - Varints: 1-9 bytes, big-endian, MSB continuation

#include "sqlite_writer.h"
#include "foundation/constants.h"
#include "foundation/compat_thread.h"
#include "foundation/profile.h"

#include <stddef.h> // NULL
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#define CBM_PAGE_SIZE 65536

/* SQLite reserves the page containing the 1 GiB file offset (the "pending byte"
 * used for file locking on Windows). This page MUST be skipped during allocation
 * otherwise integrity_check reports "2nd reference to page N" because it marks
 * this page as referenced before walking any tree.
 *
 * PENDING_BYTE = 0x40000000 = 1073741824 (1 GiB)
 * PENDING_BYTE_PAGE = (PENDING_BYTE / page_size) + 1
 *   64KB pages → page 16385
 *   32KB pages → page 32769
 *   16KB pages → page 65537
 */
#define SQLITE_MAX_PAGE_SIZE 65536
#define CBM_PENDING_BYTE (0x40000000u)
#define CBM_PENDING_BYTE_PAGE ((CBM_PENDING_BYTE / CBM_PAGE_SIZE) + 1)

/* Skip the pending byte page if allocation lands on it. */
static inline uint32_t cbm_skip_pending_byte(uint32_t pgno) {
    return pgno == CBM_PENDING_BYTE_PAGE ? pgno + SKIP_ONE : pgno;
}
#define SCHEMA_FORMAT 4
#define FILE_FORMAT 1
#define SQLITE_VERSION 3046000 // 3.46.0

// Varint encoding constants.
#define VARINT_MASK 0x7f
#define VARINT_CONTINUE 0x80
#define BYTE_MASK 0xff

enum {
    VARINT_SHIFT = 7,
    VARINT_BUF_SIZE = 10,
    VARINT_MIN_LEN = 1,
    SERIAL_INT8 = 1,
    SERIAL_INT16 = 2,
    SERIAL_INT24 = 3,
    SERIAL_INT32 = 4,
    SERIAL_INT48 = 5,
    SERIAL_INT64 = 6,
    SERIAL_FLOAT64 = 7,
    SERIAL_CONST_ZERO = 8,
    SERIAL_CONST_ONE = 9,
    SERIAL_SIZE_INT8 = 1,
    SERIAL_SIZE_INT16 = 2,
    SERIAL_SIZE_INT24 = 3,
    SERIAL_SIZE_INT32 = 4,
    SERIAL_SIZE_INT48 = 6,
    SERIAL_SIZE_INT64 = 8,
    BTREE_HEADER_SIZE = 8,
    BTREE_INTERIOR_HDR = 12,
    BTREE_PTR_SIZE = 4,
    CELL_PTR_SIZE = 2,
    INITIAL_PAGE_CAP = 4096,
    INITIAL_LEAF_CAP = 256,
    INITIAL_PARENT_CAP = 64,
    GROWTH_FACTOR = 2,
    VARINT_MAX_BYTES = 9,
    INT64_BYTES = 8,
    SORT_THRESHOLD = 20,
    MAX_NAME_LEN = 64,
    HASH_INIT = 5381,
    HASH_MULT = 33,
    HDR_FREEBLOCK_OFF = 1,
    HDR_CELLCOUNT_OFF = 3,
    HDR_CONTENT_OFF = 5,
    HDR_FRAGBYTES_OFF = 7,
    HDR_RIGHTCHILD_OFF = 8,
    INTERIOR_TABLE_FLAG = 0x05,
    INTERIOR_INDEX_FLAG = 0x02,
    NEWLINE_BYTE = 0x0A,
    NODE_SORT_THREADS = 4,
    EDGE_SORT_THREADS = 7,
    TOTAL_SORT_THREADS = 11,
    ERR_SORT_FAILED = -4,
    ERR_WRITE_FAILED = -3,
    ERR_MASTER_OVERFLOW = -2,
    MAX_EMBED_FRACTION = 64,
    MIN_EMBED_FRACTION = 32,
    LEAF_PAYLOAD_FRACTION = 32,
    INTERIOR_CELL_BUF = 20,
    FIRST_ROWID = 1,
    FIRST_DATA_PAGE = 2,
    NSORT_NAME = 1,
    NSORT_FILE = 2,
    NSORT_QN = 3,
    ESORT_TARGET = 1,
    ESORT_TYPE = 2,
    ESORT_PROJ_TGT_TYPE = 3,
    ESORT_PROJ_SRC_TYPE = 4,
    ESORT_URL_PATH = 5,
    ESORT_SRC_TGT_TYPE = 6,
    SQLITE_HEADER_SIZE = 100,
    SHIFT_8 = 8,
    SHIFT_16 = 16,
    SHIFT_24 = 24,
};
#define TEXT_SERIAL_BASE 13

// SQLite text serial type offset: serial_type = len*2 + TEXT_SERIAL_BASE.
#define TEXT_SERIAL_BASE 13

// SQLite blob serial type offset: serial_type = len*2 + BLOB_SERIAL_BASE.
#define BLOB_SERIAL_BASE 12
#define BLOB_SERIAL_MUL 2 /* serial_type = len * BLOB_SERIAL_MUL + BLOB_SERIAL_BASE */

// SQLite integer storage range limits.
#define INT8_MAX_VAL 127
#define INT16_MAX_VAL 32767
#define INT24_MIN_VAL (-8388608)
#define INT24_MAX_VAL 8388607
#define INT32_MIN_VAL (-2147483648LL)
#define INT32_MAX_VAL 2147483647LL
#define INT48_MIN_VAL (-140737488355328LL)
#define INT48_MAX_VAL 140737488355327LL

// SQLite B-tree page type flags.
#define BTREE_LEAF_TABLE 0x0D
#define BTREE_INTERIOR_TABLE 0x05
#define BTREE_LEAF_INDEX 0x0A
#define BTREE_INTERIOR_INDEX 0x02

// SQLite 100-byte database header field offsets.
#define HDR_OFF_CBM_PAGE_SIZE 16
#define HDR_OFF_WRITE_VERSION 18
#define HDR_OFF_READ_VERSION 19
#define HDR_OFF_RESERVED 20
#define HDR_OFF_MAX_EMBED_FRAC 21
#define HDR_OFF_MIN_EMBED_FRAC 22
#define HDR_OFF_LEAF_FRAC 23
#define HDR_OFF_FILE_CHANGE 24
#define HDR_OFF_DB_SIZE 28
#define HDR_OFF_FREELIST_TRUNK 32
#define HDR_OFF_FREELIST_COUNT 36
#define HDR_OFF_SCHEMA_COOKIE 40
#define HDR_OFF_SCHEMA_FORMAT 44
#define HDR_OFF_DEFAULT_CACHE 48
#define HDR_OFF_AUTOVAC_TOP 52
#define HDR_OFF_TEXT_ENCODING 56
#define HDR_OFF_USER_VERSION 60
#define HDR_OFF_INCR_VACUUM 64
#define HDR_OFF_APP_ID 68
#define HDR_OFF_VERSION_VALID 92
#define HDR_OFF_SQLITE_VERSION 96

// --- Varint encoding ---

static int put_varint(uint8_t *buf, int64_t value) {
    uint64_t v = (uint64_t)value;
    if (v <= VARINT_MASK) {
        buf[0] = (uint8_t)v;
        return SERIAL_SIZE_INT8;
    }
    // Encode in big-endian with MSB continuation bits
    uint8_t tmp[VARINT_BUF_SIZE];
    int n = 0;
    while (v > VARINT_MASK) {
        tmp[n++] = (uint8_t)(v & VARINT_MASK);
        v >>= VARINT_SHIFT;
    }
    tmp[n++] = (uint8_t)v;
    // Reverse into output with continuation bits
    for (int i = 0; i < n; i++) {
        buf[i] = tmp[n - SKIP_ONE - i];
        if (i < n - SKIP_ONE) {
            buf[i] |= VARINT_CONTINUE;
        }
    }
    return n;
}

static int varint_len(int64_t value) {
    uint64_t v = (uint64_t)value;
    int n = VARINT_MIN_LEN;
    while (v > VARINT_MASK) {
        v >>= VARINT_SHIFT;
        n++;
    }
    return n;
}

// SQLite serial type for a TEXT value
static int64_t text_serial_type(int len) {
    return (len * PAIR_LEN) + TEXT_SERIAL_BASE;
}

// SQLite serial type for an integer value
static int64_t int_serial_type(int64_t val) {
    if (val == 0) {
        return SERIAL_CONST_ZERO;
    }
    if (val == SERIAL_INT8) {
        return SERIAL_CONST_ONE;
    }
    if (val >= -INT8_MAX_VAL - SKIP_ONE && val <= INT8_MAX_VAL) {
        return SERIAL_SIZE_INT8;
    }
    if (val >= -INT16_MAX_VAL - SKIP_ONE && val <= INT16_MAX_VAL) {
        return SERIAL_SIZE_INT16;
    }
    if (val >= INT24_MIN_VAL && val <= INT24_MAX_VAL) {
        return SERIAL_SIZE_INT24;
    }
    if (val >= INT32_MIN_VAL && val <= INT32_MAX_VAL) {
        return SERIAL_SIZE_INT32;
    }
    if (val >= INT48_MIN_VAL && val <= INT48_MAX_VAL) {
        return SERIAL_SIZE_INT48;
    }
    return SERIAL_SIZE_INT64;
}

// Bytes needed to store an integer of given serial type
static int int_storage_bytes(int serial_type) {
    switch (serial_type) {
    case 0:
        return 0; // NULL
    case SERIAL_INT8:
        return SERIAL_SIZE_INT8;
    case SERIAL_INT16:
        return SERIAL_SIZE_INT16;
    case SERIAL_INT24:
        return SERIAL_SIZE_INT24;
    case SERIAL_INT32:
        return SERIAL_SIZE_INT32;
    case SERIAL_INT48:
        return SERIAL_SIZE_INT48;
    case SERIAL_INT64:
        return SERIAL_SIZE_INT64;
    case SERIAL_CONST_ZERO: // integer 0
    case SERIAL_CONST_ONE:  // integer 1
    default:
        return 0;
    }
}

// Write integer in big-endian for given byte count
static void put_int_be(uint8_t *buf, int64_t val, int nbytes) {
    for (int i = nbytes - SKIP_ONE; i >= 0; i--) {
        buf[i] = (uint8_t)(val & BYTE_MASK);
        val >>= SHIFT_8;
    }
}

// Write a 2-byte big-endian value
static void put_u16(uint8_t *buf, uint16_t val) {
    buf[0] = (uint8_t)(val >> SHIFT_8);
    buf[SKIP_ONE] = (uint8_t)(val & BYTE_MASK);
}

// Write a 4-byte big-endian value
static void put_u32(uint8_t *buf, uint32_t val) {
    buf[0] = (uint8_t)(val >> SHIFT_24);
    buf[SKIP_ONE] = (uint8_t)(val >> SHIFT_16);
    buf[PAIR_LEN] = (uint8_t)(val >> SHIFT_8);
    buf[SERIAL_SIZE_INT24] = (uint8_t)(val & BYTE_MASK);
}

// --- Dynamic buffer ---

typedef struct {
    uint8_t *data;
    int len;
    int cap;
} DynBuf;

static void dynbuf_init(DynBuf *b) {
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
}

static bool dynbuf_ensure(DynBuf *b, int needed) {
    if (b->len + needed <= b->cap) {
        return true;
    }
    int newcap = b->cap == 0 ? INITIAL_PAGE_CAP : b->cap;
    while (newcap < b->len + needed) {
        newcap *= GROWTH_FACTOR;
    }
    uint8_t *p = (uint8_t *)realloc(b->data, newcap);
    if (!p) {
        (void)fprintf(stderr, "cbm_write_db: dynbuf realloc failed size=%d\n", newcap);
        return false;
    }
    b->data = p;
    b->cap = newcap;
    return true;
}

static bool dynbuf_append(DynBuf *b, const void *data, int len) {
    if (len <= 0) {
        return true;
    }
    if (!data) {
        return false;
    }
    if (!dynbuf_ensure(b, len)) {
        return false;
    }
    memcpy(b->data + b->len, data, len);
    b->len += len;
    return true;
}

static void dynbuf_free(DynBuf *b) {
    free(b->data);
    b->data = NULL;
    b->len = b->cap = 0;
}

// --- Record builder ---
// Builds a SQLite record: header (header_len varint + serial types) + body (values)

typedef struct {
    DynBuf header; // serial type varints
    DynBuf body;   // column values
} RecordBuilder;

static void rec_init(RecordBuilder *r) {
    dynbuf_init(&r->header);
    dynbuf_init(&r->body);
}

static void rec_free(RecordBuilder *r) {
    dynbuf_free(&r->header);
    dynbuf_free(&r->body);
}

static void rec_add_null(RecordBuilder *r) {
    uint8_t v[SKIP_ONE] = {0};
    dynbuf_append(&r->header, v, SKIP_ONE);
}

static void rec_add_int(RecordBuilder *r, int64_t val) {
    int64_t st = int_serial_type(val);
    uint8_t vbuf[VARINT_MAX_BYTES];
    int vlen = put_varint(vbuf, st);
    dynbuf_append(&r->header, vbuf, vlen);

    int nbytes = int_storage_bytes((int)st);
    if (nbytes > 0) {
        uint8_t ibuf[INT64_BYTES];
        put_int_be(ibuf, val, nbytes);
        dynbuf_append(&r->body, ibuf, nbytes);
    }
}

static void rec_add_text(RecordBuilder *r, const char *s) {
    int slen = s ? (int)strlen(s) : 0;
    int64_t st = text_serial_type(slen);
    uint8_t vbuf[VARINT_MAX_BYTES];
    int vlen = put_varint(vbuf, st);
    dynbuf_append(&r->header, vbuf, vlen);
    if (slen > 0) {
        dynbuf_append(&r->body, s, slen);
    }
}

static void rec_add_blob(RecordBuilder *r, const uint8_t *data, int len) {
    int64_t st = len > 0 ? ((int64_t)len * BLOB_SERIAL_MUL) + BLOB_SERIAL_BASE : 0;
    uint8_t vbuf[VARINT_MAX_BYTES];
    int vlen = put_varint(vbuf, st);
    dynbuf_append(&r->header, vbuf, vlen);
    if (len > 0 && data) {
        dynbuf_append(&r->body, data, len);
    }
}

// Finalize: returns the complete record bytes (header_len + header + body).
// Caller must free the returned buffer.
static uint8_t *rec_finalize(RecordBuilder *r, int *out_len) {
    *out_len = 0;
    int header_content_len = r->header.len;
    int header_len_varint_len = varint_len(header_content_len + varint_len(header_content_len));
    // The header size varint includes itself, so we may need to iterate
    int total_header = header_len_varint_len + header_content_len;
    // Check if the header_len varint changes size when it includes itself
    int recalc = varint_len(total_header);
    if (recalc != header_len_varint_len) {
        header_len_varint_len = recalc;
        total_header = header_len_varint_len + header_content_len;
    }

    int total = total_header + r->body.len;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) {
        return NULL;
    }
    int pos = put_varint(buf, total_header);
    memcpy(buf + pos, r->header.data, header_content_len);
    pos += header_content_len;
    memcpy(buf + pos, r->body.data, r->body.len);
    *out_len = total;
    return buf;
}

// --- Page builder ---
// Accumulates cells (records) into B-tree leaf pages.

typedef struct {
    uint32_t page_num; // page number of this page (1-based)
    int64_t max_key;   // max rowid on this page (table B-trees)
    uint8_t *sep_cell; // separator cell content for index interior pages (owned, NULL for table)
    int sep_cell_len;
} PageRef;

typedef struct {
    FILE *fp;
    uint32_t next_page; // next page number to allocate
    int page1_offset;   // 100 for page 1, 0 for others
    bool is_index;      // true for index B-trees

    // Current leaf page being built
    uint8_t page[CBM_PAGE_SIZE];
    int cell_count;
    int content_offset; // where cell content starts (grows down from page end)
    int ptr_offset;     // where cell pointers are written (grows up from header)

    // Completed leaf pages for building interior nodes
    PageRef *leaves;
    int leaf_count;
    int leaf_cap;
} PageBuilder;

static void pb_init(PageBuilder *pb, FILE *fp, uint32_t start_page, bool is_index) {
    pb->fp = fp;
    pb->next_page = start_page;
    pb->is_index = is_index;
    pb->cell_count = 0;
    pb->content_offset = CBM_PAGE_SIZE;
    pb->page1_offset = (start_page == SKIP_ONE) ? SQLITE_HEADER_SIZE : 0;
    // Header: flag(1) + freeblock(2) + cell_count(2) + content_start(2) + fragmented(1) = 8
    pb->ptr_offset = pb->page1_offset + BTREE_HEADER_SIZE;
    memset(pb->page, 0, CBM_PAGE_SIZE);
    pb->leaves = NULL;
    pb->leaf_count = 0;
    pb->leaf_cap = 0;
}

static void pb_free(PageBuilder *pb) {
    if (pb->leaves) {
        for (int i = 0; i < pb->leaf_count; i++) {
            free(pb->leaves[i].sep_cell);
        }
        free(pb->leaves);
    }
}

// Flush current leaf page to file
static void pb_flush_leaf(PageBuilder *pb) {
    if (pb->cell_count == 0) {
        return;
    }

    int hdr = pb->page1_offset;
    // Write leaf page header
    pb->page[hdr + 0] = pb->is_index ? BTREE_LEAF_INDEX : BTREE_LEAF_TABLE; // leaf flag
    put_u16(pb->page + hdr + HDR_FREEBLOCK_OFF, 0);                         // first freeblock
    put_u16(pb->page + hdr + HDR_CELLCOUNT_OFF, (uint16_t)pb->cell_count);
    put_u16(pb->page + hdr + HDR_CONTENT_OFF, (uint16_t)pb->content_offset);
    pb->page[hdr + HDR_FRAGBYTES_OFF] = 0; // fragmented free bytes

    // Write page to file. Skip the pending byte page (SQLite reserved).
    pb->next_page = cbm_skip_pending_byte(pb->next_page);
    uint32_t page_num = pb->next_page;
    long offset = (long)(page_num - SKIP_ONE) * CBM_PAGE_SIZE;
    (void)fseek(pb->fp, offset, SEEK_SET);
    (void)fwrite(pb->page, SKIP_ONE, CBM_PAGE_SIZE, pb->fp);

    // Record this leaf for interior page building
    if (pb->leaf_count >= pb->leaf_cap) {
        int old_cap = pb->leaf_cap;
        pb->leaf_cap = old_cap == 0 ? INITIAL_LEAF_CAP : old_cap * GROWTH_FACTOR;
        void *tmp = realloc(pb->leaves, (size_t)pb->leaf_cap * sizeof(PageRef));
        if (!tmp) {
            free(pb->leaves);
            pb->leaves = NULL;
            return;
        }
        pb->leaves = (PageRef *)tmp;
        /* Zero-init new slots */
        memset(&pb->leaves[old_cap], 0, ((size_t)pb->leaf_cap - (size_t)old_cap) * sizeof(PageRef));
    }
    pb->leaves[pb->leaf_count].page_num = page_num;
    // max_key is set by caller before flush
    pb->leaf_count++;

    // Reset for next page
    pb->next_page++;
    pb->cell_count = 0;
    pb->content_offset = CBM_PAGE_SIZE;
    pb->page1_offset = 0;               // only page 1 has the 100-byte header
    pb->ptr_offset = BTREE_HEADER_SIZE; // standard B-tree header size for non-page-1
    memset(pb->page, 0, CBM_PAGE_SIZE);
}

// Check if a cell of given size fits in the current page
static bool pb_cell_fits(PageBuilder *pb, int cell_len) {
    // Cell pointer (2 bytes) + cell content
    int available = pb->content_offset - pb->ptr_offset - CELL_PTR_SIZE;
    return cell_len <= available;
}

// Add a cell to the current leaf page.
// For table leaves: varint(payload_len) + varint(rowid) + payload
// For index leaves: varint(payload_len) + payload
static void pb_add_cell(PageBuilder *pb, const uint8_t *cell, int cell_len) {
    // Write cell content (grows down)
    pb->content_offset -= cell_len;
    memcpy(pb->page + pb->content_offset, cell, cell_len);

    // Write cell pointer (grows up)
    put_u16(pb->page + pb->ptr_offset, (uint16_t)pb->content_offset);
    pb->ptr_offset += CELL_PTR_SIZE;
    pb->cell_count++;
}

// Build interior pages from child page references.
// Returns the root page number.
//
// SQLite interior page structure:
//   - Header has right-child pointer (the last child page)
//   - Each cell contains: child_page(4) + key
//   - For N children, there are N-1 cells (children[0..N-2] get cells,
//     children[N-1] becomes the right-child in the header)
//   - Cell[j] = {left_child: children[j].page, key: children[j].max_key/sep_cell}
//   - Lookup: X ≤ K0 → cell[0].left_child, K0 < X ≤ K1 → cell[1].left_child, etc.
//   - Table keys: varint(rowid)
//   - Index keys: varint(payload_len) + payload (full index record)
// Build an interior cell for a child PageRef. Returns cell length.
// For table B-trees: child_page(4) + varint(rowid).
// For index B-trees: child_page(4) + separator_cell.
// cell_buf must be at least 20 bytes for table cells.
// For index cells, returns malloc'd data via *out_heap (caller frees).
static int build_interior_cell(const PageRef *child, bool is_index, uint8_t *cell_buf,
                               uint8_t **out_heap) {
    *out_heap = NULL;
    if (!is_index) {
        put_u32(cell_buf, child->page_num);
        return BTREE_PTR_SIZE + put_varint(cell_buf + BTREE_PTR_SIZE, child->max_key);
    }
    int clen = BTREE_PTR_SIZE + child->sep_cell_len;
    uint8_t *data = (uint8_t *)malloc(clen);
    put_u32(data, child->page_num);
    memcpy(data + 4, child->sep_cell, child->sep_cell_len);
    *out_heap = data;
    return clen;
}

// Write a completed interior page to disk and record it as a parent.
// Returns updated parent_count, or -1 on allocation failure.
static int write_interior_page(PageBuilder *pb, uint8_t *page, int cell_count, int content_offset,
                               uint32_t right_child_page, const PageRef *children,
                               int right_child_idx, bool is_index, PageRef **parents,
                               int parent_count, int *parent_cap) {
    pb->next_page = cbm_skip_pending_byte(pb->next_page);
    uint32_t pnum = pb->next_page++;
    page[0] = is_index ? INTERIOR_INDEX_FLAG : INTERIOR_TABLE_FLAG;
    put_u16(page + HDR_FREEBLOCK_OFF, 0);
    put_u16(page + HDR_CELLCOUNT_OFF, (uint16_t)cell_count);
    put_u16(page + HDR_CONTENT_OFF, (uint16_t)content_offset);
    page[HDR_FRAGBYTES_OFF] = 0;
    put_u32(page + HDR_RIGHTCHILD_OFF, right_child_page);

    (void)fseek(pb->fp, (long)(pnum - SKIP_ONE) * CBM_PAGE_SIZE, SEEK_SET);
    (void)fwrite(page, SKIP_ONE, CBM_PAGE_SIZE, pb->fp);

    if (parent_count >= *parent_cap) {
        int old_pcap = *parent_cap;
        *parent_cap = old_pcap == 0 ? INITIAL_PARENT_CAP : old_pcap * GROWTH_FACTOR;
        PageRef *tmp = (PageRef *)realloc(*parents, *parent_cap * sizeof(PageRef));
        if (!tmp) {
            free(*parents);
            *parents = NULL;
            return CBM_NOT_FOUND;
        }
        *parents = tmp;
        memset(&(*parents)[old_pcap], 0,
               ((size_t)*parent_cap - (size_t)old_pcap) * sizeof(PageRef));
    }
    (*parents)[parent_count].page_num = pnum;
    (*parents)[parent_count].max_key = children[right_child_idx].max_key;
    if (is_index && children[right_child_idx].sep_cell) {
        int slen = children[right_child_idx].sep_cell_len;
        (*parents)[parent_count].sep_cell = (uint8_t *)malloc(slen);
        memcpy((*parents)[parent_count].sep_cell, children[right_child_idx].sep_cell, slen);
        (*parents)[parent_count].sep_cell_len = slen;
    } else {
        (*parents)[parent_count].sep_cell = NULL;
        (*parents)[parent_count].sep_cell_len = 0;
    }
    return parent_count + SKIP_ONE;
}

// Free a PageRef array (sep_cell allocations), unless it's the original leaves.
static void free_children(PageRef *children, int child_count, const PageRef *leaves) {
    if (children != leaves) {
        for (int j = 0; j < child_count; j++) {
            free(children[j].sep_cell);
        }
        free(children);
    }
}

// Fill an interior page with cells from children[*idx..child_count-2].
// Updates cell_count, content_offset, ptr_offset, and *idx.
static void fill_interior_page(uint8_t *page, const PageRef *children, int child_count,
                               bool is_index, int *idx, int *cell_count, int *content_offset,
                               int *ptr_offset) {
    while (*idx < child_count - SKIP_ONE) {
        uint8_t tbuf[INTERIOR_CELL_BUF];
        uint8_t *heap_cell = NULL;
        int clen = build_interior_cell(&children[*idx], is_index, tbuf, &heap_cell);
        uint8_t *cell_data = heap_cell ? heap_cell : tbuf;

        int available = *content_offset - *ptr_offset - CELL_PTR_SIZE;
        if (clen > available && *cell_count > 0) {
            free(heap_cell);
            break;
        }

        *content_offset -= clen;
        memcpy(page + *content_offset, cell_data, clen);
        put_u16(page + *ptr_offset, (uint16_t)*content_offset);
        *ptr_offset += CELL_PTR_SIZE;
        (*cell_count)++;
        free(heap_cell);
        (*idx)++;
    }
}

static uint32_t pb_build_interior(PageBuilder *pb, bool is_index) {
    if (!pb->leaves) {
        return 0;
    }
    if (pb->leaf_count <= SKIP_ONE) {
        return pb->leaves[0].page_num;
    }

    PageRef *children = pb->leaves;
    int child_count = pb->leaf_count;

    while (child_count > SKIP_ONE && children) {
        PageRef *parents = NULL;
        int parent_count = 0;
        int parent_cap = 0;

        int i = 0;
        while (i < child_count) {
            uint8_t page[CBM_PAGE_SIZE];
            memset(page, 0, CBM_PAGE_SIZE);
            int cell_count = 0;
            int content_offset = CBM_PAGE_SIZE;
            int ptr_offset = BTREE_INTERIOR_HDR;

            fill_interior_page(page, children, child_count, is_index, &i, &cell_count,
                               &content_offset, &ptr_offset);

            int right_child_idx = (i < child_count - SKIP_ONE) ? i : child_count - SKIP_ONE;
            uint32_t right_child_page = 0;
            if (right_child_idx >= 0 && right_child_idx < child_count) {
                right_child_page = children[right_child_idx].page_num;
            }
            if (i < child_count - SKIP_ONE) {
                i++;
            } else {
                i = child_count;
            }

            parent_count = write_interior_page(pb, page, cell_count, content_offset,
                                               right_child_page, children, right_child_idx,
                                               is_index, &parents, parent_count, &parent_cap);
            if (parent_count < 0) {
                break;
            }
        }

        free_children(children, child_count, pb->leaves);
        children = parents;
        child_count = parent_count;
    }

    uint32_t root = children ? children[0].page_num : 0;
    free_children(children, child_count, pb->leaves);
    return root;
}

// --- Table record builders ---

// Build a nodes table record: (id, project, label, name, qualified_name, file_path, start_line,
// end_line, properties)
static uint8_t *build_node_record(const CBMDumpNode *n, int *out_len) {
    RecordBuilder r;
    rec_init(&r);

    rec_add_int(&r, n->id);
    rec_add_text(&r, n->project);
    rec_add_text(&r, n->label);
    rec_add_text(&r, n->name);
    rec_add_text(&r, n->qualified_name);
    rec_add_text(&r, n->file_path ? n->file_path : "");
    rec_add_int(&r, n->start_line);
    rec_add_int(&r, n->end_line);
    rec_add_text(&r, n->properties ? n->properties : "{}");

    uint8_t *data = rec_finalize(&r, out_len);
    rec_free(&r);
    return data;
}

// Build an edges table record: (id, project, source_id, target_id, type, properties)
// url_path_gen is a VIRTUAL generated column — NOT stored in the record.
static uint8_t *build_edge_record(const CBMDumpEdge *e, int *out_len) {
    RecordBuilder r;
    rec_init(&r);

    rec_add_int(&r, e->id);
    rec_add_text(&r, e->project);
    rec_add_int(&r, e->source_id);
    rec_add_int(&r, e->target_id);
    rec_add_text(&r, e->type);
    rec_add_text(&r, e->properties ? e->properties : "{}");

    uint8_t *data = rec_finalize(&r, out_len);
    rec_free(&r);
    return data;
}

// Build a node_vectors table record: (node_id, project, vector)
// Includes node_id in the record body (same pattern as build_node_record).
static uint8_t *build_vector_record(const CBMDumpVector *v, int *out_len) {
    RecordBuilder r;
    rec_init(&r);

    rec_add_int(&r, v->node_id);
    rec_add_text(&r, v->project);
    rec_add_blob(&r, v->vector, v->vector_len);

    uint8_t *data = rec_finalize(&r, out_len);
    rec_free(&r);
    return data;
}

// Build a token_vectors table record: (id, project, token, vector, idf)
static uint8_t *build_token_vec_record(const CBMDumpTokenVec *tv, int *out_len) {
    RecordBuilder r;
    rec_init(&r);

    rec_add_int(&r, tv->id);
    rec_add_text(&r, tv->project);
    rec_add_text(&r, tv->token);
    rec_add_blob(&r, tv->vector, tv->vector_len);
    /* Store IDF as integer × 1000 for fixed-point (avoid float in record) */
    enum { IDF_FIXED_POINT_SCALE = 1000 };
    rec_add_int(&r, (int64_t)(tv->idf * IDF_FIXED_POINT_SCALE));

    uint8_t *data = rec_finalize(&r, out_len);
    rec_free(&r);
    return data;
}

// Build a projects table record: (name, indexed_at, root_path)
static uint8_t *build_project_record(const char *name, const char *indexed_at,
                                     const char *root_path, int *out_len) {
    RecordBuilder r;
    rec_init(&r);

    rec_add_text(&r, name);
    rec_add_text(&r, indexed_at);
    rec_add_text(&r, root_path);

    uint8_t *data = rec_finalize(&r, out_len);
    rec_free(&r);
    return data;
}

// --- Table cell builder ---
// Table leaf cell: varint(payload_len) + varint(rowid) + payload

static uint8_t *build_table_cell(int64_t rowid, const uint8_t *payload, int payload_len,
                                 int *out_cell_len) {
    int rl = varint_len(payload_len);
    int kl = varint_len(rowid);
    int total = rl + kl + payload_len;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        return NULL;
    }
    int pos = 0;
    pos += put_varint(cell + pos, payload_len);
    pos += put_varint(cell + pos, rowid);
    memcpy(cell + pos, payload, payload_len);
    *out_cell_len = pos + payload_len;
    return cell;
}

// Build a table leaf cell with overflow: stores only the first local_len bytes of
// payload inline, followed by a 4-byte overflow page number.
// total_payload_len is the FULL original payload length (written as the payload-size
// varint so SQLite knows the real record size).
static uint8_t *build_table_cell_overflow(int64_t rowid, const uint8_t *payload,
                                          int total_payload_len, int local_len,
                                          uint32_t overflow_page, int *out_cell_len) {
    int rl = varint_len(total_payload_len);
    int kl = varint_len(rowid);
    // cell = varint(total_payload_len) + varint(rowid) + payload[0..local_len) + uint32(overflow)
    int total = rl + kl + local_len + BTREE_PTR_SIZE;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        return NULL;
    }
    int pos = 0;
    pos += put_varint(cell + pos, total_payload_len);
    pos += put_varint(cell + pos, rowid);
    memcpy(cell + pos, payload, local_len);
    pos += local_len;
    put_u32(cell + pos, overflow_page);
    pos += BTREE_PTR_SIZE;
    *out_cell_len = pos;
    return cell;
}

// --- Overflow page writer ---
// Writes overflow pages for payload bytes that exceed local storage.
// Returns the first overflow page number (embedded in the leaf cell).
// Each overflow page: 4-byte next-page pointer + up to (CBM_PAGE_SIZE-4) bytes of data.
static uint32_t write_overflow_pages(FILE *fp, uint32_t *next_page, const uint8_t *data,
                                     int data_len) {
    int per_page = CBM_PAGE_SIZE - BTREE_PTR_SIZE;
    uint32_t first_page = 0;
    long prev_next_ptr_offset = -SKIP_ONE;

    int offset = 0;
    while (offset < data_len) {
        uint32_t pnum = (*next_page)++;
        if (first_page == 0) {
            first_page = pnum;
        }

        // Backpatch previous overflow page's next-page pointer
        if (prev_next_ptr_offset >= 0) {
            uint8_t ptr[BTREE_PTR_SIZE];
            put_u32(ptr, pnum);
            (void)fseek(fp, prev_next_ptr_offset, SEEK_SET);
            (void)fwrite(ptr, SKIP_ONE, BTREE_PTR_SIZE, fp);
        }

        int chunk = data_len - offset;
        if (chunk > per_page) {
            chunk = per_page;
        }

        uint8_t page[CBM_PAGE_SIZE];
        memset(page, 0, CBM_PAGE_SIZE);
        put_u32(page, 0); // next-page pointer — 0 for now, backpatched on next iteration
        memcpy(page + BTREE_PTR_SIZE, data + offset, chunk);

        long page_offset = (long)(pnum - SKIP_ONE) * CBM_PAGE_SIZE;
        prev_next_ptr_offset = page_offset;
        (void)fseek(fp, page_offset, SEEK_SET);
        (void)fwrite(page, SKIP_ONE, CBM_PAGE_SIZE, fp);

        offset += chunk;
    }
    return first_page;
}

// --- Index record builders ---

// Build an index entry for a 2-column TEXT index (project, col) + rowid.
// Index records: varint(payload_len) + payload(record of indexed cols + rowid)
static uint8_t *build_index_entry_2text_rowid(const char *col1, const char *col2, int64_t rowid,
                                              int *out_len) {
    // Build the record portion: (col1, col2, rowid)
    RecordBuilder r;
    rec_init(&r);
    rec_add_text(&r, col1);
    rec_add_text(&r, col2);
    rec_add_int(&r, rowid);
    int payload_len = 0;
    uint8_t *payload = rec_finalize(&r, &payload_len);
    rec_free(&r);
    if (!payload) {
        *out_len = 0;
        return NULL;
    }

    // Index cell: varint(payload_len) + payload
    int vl = varint_len(payload_len);
    int total = vl + payload_len;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        free(payload);
        *out_len = 0;
        return NULL;
    }
    int pos = put_varint(cell, payload_len);
    memcpy(cell + pos, payload, payload_len);
    free(payload);
    *out_len = total;
    return cell;
}

// Build index entry for (int64, text) + rowid (e.g., idx_edges_source)
static uint8_t *build_index_entry_int_text_rowid(int64_t val, const char *text, int64_t rowid,
                                                 int *out_len) {
    RecordBuilder r;
    rec_init(&r);
    rec_add_int(&r, val);
    rec_add_text(&r, text);
    rec_add_int(&r, rowid);
    int payload_len = 0;
    uint8_t *payload = rec_finalize(&r, &payload_len);
    rec_free(&r);
    if (!payload) {
        *out_len = 0;
        return NULL;
    }

    int vl = varint_len(payload_len);
    int total = vl + payload_len;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        free(payload);
        *out_len = 0;
        return NULL;
    }
    int pos = put_varint(cell, payload_len);
    memcpy(cell + pos, payload, payload_len);
    free(payload);
    *out_len = total;
    return cell;
}

// Build index entry for (text, int64, text) + rowid (e.g., idx_edges_target_type)
static uint8_t *build_index_entry_text_int_text_rowid(const char *t1, int64_t val, const char *t2,
                                                      int64_t rowid, int *out_len) {
    RecordBuilder r;
    rec_init(&r);
    rec_add_text(&r, t1);
    rec_add_int(&r, val);
    rec_add_text(&r, t2);
    rec_add_int(&r, rowid);
    int payload_len = 0;
    uint8_t *payload = rec_finalize(&r, &payload_len);
    rec_free(&r);
    if (!payload) {
        *out_len = 0;
        return NULL;
    }

    int vl = varint_len(payload_len);
    int total = vl + payload_len;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        free(payload);
        *out_len = 0;
        return NULL;
    }
    int pos = put_varint(cell, payload_len);
    memcpy(cell + pos, payload, payload_len);
    free(payload);
    *out_len = total;
    return cell;
}

// Build UNIQUE index entry for (text, text) + rowid (e.g., nodes unique(project, qualified_name))
// Build UNIQUE index entry for (int64, int64, text) + rowid (edges unique(source_id, target_id,
// type))
static uint8_t *build_index_entry_unique_2int_text_rowid(int64_t v1, int64_t v2, const char *text,
                                                         int64_t rowid, int *out_len) {
    RecordBuilder r;
    rec_init(&r);
    rec_add_int(&r, v1);
    rec_add_int(&r, v2);
    rec_add_text(&r, text);
    rec_add_int(&r, rowid);
    int payload_len = 0;
    uint8_t *payload = rec_finalize(&r, &payload_len);
    rec_free(&r);
    if (!payload) {
        *out_len = 0;
        return NULL;
    }

    int vlen = varint_len(payload_len);
    int total = vlen + payload_len;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        free(payload);
        *out_len = 0;
        return NULL;
    }
    int pos = put_varint(cell, payload_len);
    memcpy(cell + pos, payload, payload_len);
    free(payload);
    *out_len = total;
    return cell;
}

// --- Write a table B-tree from records ---

// Ensure leaves array has capacity for one more entry.
// Returns false on allocation failure.
static bool pb_ensure_leaf_cap(PageBuilder *pb) {
    if (pb->leaf_count < pb->leaf_cap) {
        return true;
    }
    pb->leaf_cap = pb->leaf_cap == 0 ? INITIAL_LEAF_CAP : pb->leaf_cap * GROWTH_FACTOR;
    void *tmp = realloc(pb->leaves, (size_t)pb->leaf_cap * sizeof(PageRef));
    if (!tmp) {
        free(pb->leaves);
        pb->leaves = NULL;
        return false;
    }
    pb->leaves = (PageRef *)tmp;
    return true;
}

// SQLite overflow thresholds for leaf table B-tree pages (PAGE_SIZE=65536, reserved=0):
//   usable    = PAGE_SIZE = 65536
//   max_local = usable - 35 = 65501
//   min_local = (usable - 12) * 32 / 255 - 23 = 8199  (C integer arithmetic, same as SQLite)
#define TABLE_OVERFLOW_MAX_LOCAL 65501
#define TABLE_OVERFLOW_MIN_LOCAL 8199

// Add a table cell to the PageBuilder, flushing leaf pages as needed.
// If the payload exceeds max_local, overflow pages are written and only the
// local portion plus a 4-byte overflow page pointer is stored in the leaf cell.
static void pb_add_table_cell_with_flush(PageBuilder *pb, int64_t rowid, const uint8_t *payload,
                                         int payload_len, int64_t prev_rowid) {
    int cell_len = 0;
    uint8_t *cell = NULL;

    if (payload_len > TABLE_OVERFLOW_MAX_LOCAL) {
        // Compute local_len per SQLite spec for leaf table cells.
        int ovfl_page_data = CBM_PAGE_SIZE - BTREE_PTR_SIZE;
        int remainder = (payload_len - TABLE_OVERFLOW_MIN_LOCAL) % ovfl_page_data;
        int local_len = TABLE_OVERFLOW_MIN_LOCAL + remainder;
        if (local_len > TABLE_OVERFLOW_MAX_LOCAL) {
            local_len = TABLE_OVERFLOW_MIN_LOCAL;
        }

        // Write overflow pages for the bytes that don't fit locally.
        uint32_t overflow_page = write_overflow_pages(pb->fp, &pb->next_page, payload + local_len,
                                                      payload_len - local_len);
        if (overflow_page == 0) {
            return; // overflow write failed
        }

        cell = build_table_cell_overflow(rowid, payload, payload_len, local_len, overflow_page,
                                         &cell_len);
    } else {
        cell = build_table_cell(rowid, payload, payload_len, &cell_len);
    }

    if (!cell) {
        return;
    }

    if (!pb_cell_fits(pb, cell_len) && pb->cell_count > 0) {
        if (!pb_ensure_leaf_cap(pb)) {
            free(cell);
            return;
        }
        pb->leaves[pb->leaf_count].max_key = prev_rowid;
        pb->leaves[pb->leaf_count].sep_cell = NULL;
        pb->leaves[pb->leaf_count].sep_cell_len = 0;
        pb_flush_leaf(pb);
    }

    pb_add_cell(pb, cell, cell_len);
    free(cell);
}

// Finalize a table PageBuilder: flush last leaf and build interior pages.
static uint32_t pb_finalize_table(PageBuilder *pb, uint32_t *next_page, int64_t last_rowid) {
    if (pb->cell_count > 0) {
        pb_ensure_leaf_cap(pb);
        if (!pb->leaves) {
            pb_free(pb);
            return 0;
        }
        pb->leaves[pb->leaf_count].max_key = last_rowid;
        pb->leaves[pb->leaf_count].sep_cell = NULL;
        pb->leaves[pb->leaf_count].sep_cell_len = 0;
        pb_flush_leaf(pb);
    }

    *next_page = pb->next_page;
    uint32_t root;
    if (pb->leaf_count == SKIP_ONE) {
        root = pb->leaves[0].page_num;
    } else if (pb->leaf_count > SKIP_ONE) {
        root = pb_build_interior(pb, false);
        *next_page = pb->next_page;
    } else {
        root = 0; // shouldn't happen when count > 0
    }
    pb_free(pb);
    return root;
}

// Write leaf pages for a table, returns root page.
// rowids must be sequential starting from 1 (or single-row PK text).
static uint32_t write_table_btree(FILE *fp, uint32_t *next_page, const uint8_t **records,
                                  const int *record_lens, const int64_t *rowids, int count,
                                  bool first_is_page1) {
    if (count == 0) {
        // Empty table: write a single empty leaf page
        *next_page = cbm_skip_pending_byte(*next_page);
        uint32_t pnum = (*next_page)++;
        uint8_t page[CBM_PAGE_SIZE];
        memset(page, 0, CBM_PAGE_SIZE);
        int hdr = first_is_page1 ? SQLITE_HEADER_SIZE : 0;
        page[hdr] = BTREE_LEAF_TABLE;                                   // leaf table
        put_u16(page + hdr + HDR_FREEBLOCK_OFF, 0);                     // no freeblocks
        put_u16(page + hdr + HDR_CELLCOUNT_OFF, 0);                     // 0 cells
        put_u16(page + hdr + HDR_CONTENT_OFF, (uint16_t)CBM_PAGE_SIZE); // content at end of page
        page[hdr + HDR_FRAGBYTES_OFF] = 0;                              // 0 fragmented bytes
        (void)fseek(fp, (long)(pnum - SKIP_ONE) * CBM_PAGE_SIZE, SEEK_SET);
        (void)fwrite(page, SKIP_ONE, CBM_PAGE_SIZE, fp);
        return pnum;
    }

    PageBuilder pb;
    pb_init(&pb, fp, *next_page, false);
    pb.page1_offset = first_is_page1 ? SQLITE_HEADER_SIZE : 0;
    pb.ptr_offset = pb.page1_offset + BTREE_HEADER_SIZE;

    for (int i = 0; i < count; i++) {
        pb_add_table_cell_with_flush(&pb, rowids[i], records[i], record_lens[i],
                                     i > 0 ? rowids[i - SKIP_ONE] : 0);
    }

    return pb_finalize_table(&pb, next_page, rowids[count - SKIP_ONE]);
}

// Promote the last cell from current page to separator, un-add it, and flush.
static bool pb_promote_and_flush(PageBuilder *pb, uint8_t **cells, int *cell_lens, int prev_idx) {
    if (!pb_ensure_leaf_cap(pb)) {
        return false;
    }
    pb->leaves[pb->leaf_count].max_key = 0;
    pb->leaves[pb->leaf_count].sep_cell = (uint8_t *)malloc(cell_lens[prev_idx]);
    memcpy(pb->leaves[pb->leaf_count].sep_cell, cells[prev_idx], cell_lens[prev_idx]);
    pb->leaves[pb->leaf_count].sep_cell_len = cell_lens[prev_idx];

    // Un-add the last cell — it's promoted to the interior separator.
    // SQLite index B-tree interior cells are counted by integrity_check,
    // so this cell exists in the interior page instead of the leaf.
    pb->cell_count--;
    pb->content_offset += cell_lens[prev_idx];
    pb->ptr_offset -= CELL_PTR_SIZE;

    pb_flush_leaf(pb);
    return true;
}

// Write an empty index leaf page.
static uint32_t write_empty_index_leaf(FILE *fp, uint32_t *next_page) {
    *next_page = cbm_skip_pending_byte(*next_page);
    uint32_t pnum = (*next_page)++;
    uint8_t page[CBM_PAGE_SIZE];
    memset(page, 0, CBM_PAGE_SIZE);
    page[0] = NEWLINE_BYTE;
    put_u16(page + HDR_FREEBLOCK_OFF, 0);
    put_u16(page + HDR_CELLCOUNT_OFF, 0);
    put_u16(page + HDR_CONTENT_OFF, (uint16_t)CBM_PAGE_SIZE);
    page[HDR_FRAGBYTES_OFF] = 0;
    (void)fseek(fp, (long)(pnum - SKIP_ONE) * CBM_PAGE_SIZE, SEEK_SET);
    (void)fwrite(page, SKIP_ONE, CBM_PAGE_SIZE, fp);
    return pnum;
}

// Write leaf pages for an index, returns root page.
static uint32_t write_index_btree(FILE *fp, uint32_t *next_page, uint8_t **cells, int *cell_lens,
                                  int count) {
    if (count == 0) {
        return write_empty_index_leaf(fp, next_page);
    }

    PageBuilder pb;
    pb_init(&pb, fp, *next_page, true);

    for (int i = 0; i < count; i++) {
        if (!pb_cell_fits(&pb, cell_lens[i])) {
            if (pb.cell_count > 0) {
                if (!pb_promote_and_flush(&pb, cells, cell_lens, i - SKIP_ONE)) {
                    return 0;
                }
            }
            // After flush, check if the cell still doesn't fit on an empty page.
            // Index cells larger than a full page can never be stored; skip them.
            if (!pb_cell_fits(&pb, cell_lens[i])) {
                continue;
            }
        }
        pb_add_cell(&pb, cells[i], cell_lens[i]);
    }

    if (pb.cell_count > 0) {
        if (!pb_ensure_leaf_cap(&pb)) {
            return 0;
        }
        pb.leaves[pb.leaf_count].max_key = 0;
        int last = count - SKIP_ONE;
        pb.leaves[pb.leaf_count].sep_cell = (uint8_t *)malloc(cell_lens[last]);
        memcpy(pb.leaves[pb.leaf_count].sep_cell, cells[last], cell_lens[last]);
        pb.leaves[pb.leaf_count].sep_cell_len = cell_lens[last];
        pb_flush_leaf(&pb);
    }

    *next_page = pb.next_page;

    uint32_t root;
    if (!pb.leaves) {
        root = 0;
    } else if (pb.leaf_count == SKIP_ONE) {
        root = pb.leaves[0].page_num;
    } else {
        root = pb_build_interior(&pb, true);
        *next_page = pb.next_page;
    }

    pb_free(&pb);
    return root;
}

// --- sqlite_master entries ---

typedef struct {
    const char *type;     // "table" or "index"
    const char *name;     // table/index name
    const char *tbl_name; // table name
    uint32_t rootpage;    // root page number
    const char *sql;      // CREATE statement
} MasterEntry;

static uint8_t *build_master_record(const MasterEntry *e, int *out_len) {
    RecordBuilder r;
    rec_init(&r);
    rec_add_text(&r, e->type);
    rec_add_text(&r, e->name);
    rec_add_text(&r, e->tbl_name);
    rec_add_int(&r, (int64_t)e->rootpage);
    if (e->sql) {
        rec_add_text(&r, e->sql);
    } else {
        rec_add_null(&r);
    }
    uint8_t *data = rec_finalize(&r, out_len);
    rec_free(&r);
    return data;
}

// --- qsort comparators for index sorting ---
// Single-threaded writer: static context is safe.

static const CBMDumpNode *g_sort_nodes;
static const CBMDumpEdge *g_sort_edges;

static inline int cmp_i64(int64_t a, int64_t b) {
    return (a > b) - (a < b);
}

static inline const char *safe_str(const char *s) {
    return s ? s : "";
}

// Allocate permutation array [0, 1, ..., n-1], sort with comparator.
// Returns NULL on allocation failure.
static int *make_sorted_perm(int n, int (*cmp)(const void *, const void *)) {
    int *perm = (int *)malloc(n * sizeof(int));
    if (!perm) {
        (void)fprintf(stderr, "cbm_write_db: perm malloc failed n=%d size=%zu\n", n,
                      (size_t)n * sizeof(int));
        return NULL;
    }
    for (int i = 0; i < n; i++) {
        perm[i] = i;
    }
    qsort(perm, n, sizeof(int), cmp);
    return perm;
}

// --- Node index comparators (project is same for all, skip it) ---

static int cmp_node_by_label(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].label), safe_str(g_sort_nodes[ib].label));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

static int cmp_node_by_name(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].name), safe_str(g_sort_nodes[ib].name));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

static int cmp_node_by_file(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].file_path), safe_str(g_sort_nodes[ib].file_path));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

static int cmp_node_by_qn(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].qualified_name),
                   safe_str(g_sort_nodes[ib].qualified_name));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

// --- Edge index comparators ---

// idx_edges_source: (source_id, type) + rowid
static int cmp_edge_by_source_type(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = cmp_i64(g_sort_edges[ia].source_id, g_sort_edges[ib].source_id);
    if (c) {
        return c;
    }
    c = strcmp(safe_str(g_sort_edges[ia].type), safe_str(g_sort_edges[ib].type));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// idx_edges_target: (target_id, type) + rowid
static int cmp_edge_by_target_type(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = cmp_i64(g_sort_edges[ia].target_id, g_sort_edges[ib].target_id);
    if (c) {
        return c;
    }
    c = strcmp(safe_str(g_sort_edges[ia].type), safe_str(g_sort_edges[ib].type));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// idx_edges_type: (project, type) + rowid
static int cmp_edge_by_type(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_edges[ia].type), safe_str(g_sort_edges[ib].type));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// idx_edges_target_type: (project, target_id, type) + rowid
static int cmp_edge_by_proj_target_type(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = cmp_i64(g_sort_edges[ia].target_id, g_sort_edges[ib].target_id);
    if (c) {
        return c;
    }
    c = strcmp(safe_str(g_sort_edges[ia].type), safe_str(g_sort_edges[ib].type));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// idx_edges_source_type: (project, source_id, type) + rowid
static int cmp_edge_by_proj_source_type(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = cmp_i64(g_sort_edges[ia].source_id, g_sort_edges[ib].source_id);
    if (c) {
        return c;
    }
    c = strcmp(safe_str(g_sort_edges[ia].type), safe_str(g_sort_edges[ib].type));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// idx_edges_url_path: (project, url_path_gen) + rowid — NULL sorts first
static int cmp_edge_by_url_path(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    const char *ua = g_sort_edges[ia].url_path;
    const char *ub = g_sort_edges[ib].url_path;
    bool na = (!ua || ua[0] == '\0');
    bool nb = (!ub || ub[0] == '\0');
    if (na && nb) {
        return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
    }
    if (na) {
        return CBM_NOT_FOUND;
    }
    if (nb) {
        return SERIAL_SIZE_INT8;
    }
    int c = strcmp(ua, ub);
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// autoindex_edges_1: UNIQUE(source_id, target_id, type) + rowid
static int cmp_edge_by_src_tgt_type(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = cmp_i64(g_sort_edges[ia].source_id, g_sort_edges[ib].source_id);
    if (c) {
        return c;
    }
    c = cmp_i64(g_sort_edges[ia].target_id, g_sort_edges[ib].target_id);
    if (c) {
        return c;
    }
    c = strcmp(safe_str(g_sort_edges[ia].type), safe_str(g_sort_edges[ib].type));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// --- Parallel sort support ---

typedef struct {
    int count;
    int (*cmp)(const void *, const void *);
    int *perm; // output: sorted permutation array, caller frees
} SortJob;

static void *sort_worker(void *arg) {
    SortJob *j = (SortJob *)arg;
    j->perm = make_sorted_perm(j->count, j->cmp);
    return NULL;
}

/* Edge index cell builder callback: builds one index cell from an edge. */
typedef uint8_t *(*edge_cell_fn)(const CBMDumpEdge *e, int *out_len);

static uint8_t *ecell_source(const CBMDumpEdge *e, int *out_len) {
    return build_index_entry_int_text_rowid(e->source_id, e->type, e->id, out_len);
}
static uint8_t *ecell_target(const CBMDumpEdge *e, int *out_len) {
    return build_index_entry_int_text_rowid(e->target_id, e->type, e->id, out_len);
}
static uint8_t *ecell_type(const CBMDumpEdge *e, int *out_len) {
    return build_index_entry_2text_rowid(e->project, e->type, e->id, out_len);
}
static uint8_t *ecell_proj_target_type(const CBMDumpEdge *e, int *out_len) {
    return build_index_entry_text_int_text_rowid(e->project, e->target_id, e->type, e->id, out_len);
}
static uint8_t *ecell_proj_source_type(const CBMDumpEdge *e, int *out_len) {
    return build_index_entry_text_int_text_rowid(e->project, e->source_id, e->type, e->id, out_len);
}
static uint8_t *ecell_src_tgt_type(const CBMDumpEdge *e, int *out_len) {
    return build_index_entry_unique_2int_text_rowid(e->source_id, e->target_id, e->type, e->id,
                                                    out_len);
}
static uint8_t *ecell_url_path(const CBMDumpEdge *e, int *out_len) {
    const char *url = (e->url_path && e->url_path[0] != '\0') ? e->url_path : NULL;
    RecordBuilder r;
    rec_init(&r);
    rec_add_text(&r, e->project);
    if (url) {
        rec_add_text(&r, url);
    } else {
        rec_add_null(&r);
    }
    rec_add_int(&r, e->id);
    int payload_len = 0;
    uint8_t *payload = rec_finalize(&r, &payload_len);
    rec_free(&r);
    int vlen = varint_len(payload_len);
    int total = vlen + payload_len;
    uint8_t *cell = (uint8_t *)malloc(total);
    if (!cell) {
        free(payload);
        *out_len = 0;
        return NULL;
    }
    int pos = put_varint(cell, payload_len);
    memcpy(cell + pos, payload, payload_len);
    free(payload);
    *out_len = total;
    return cell;
}

/* Build an edge index from a pre-sorted permutation using a cell builder callback. */
static uint32_t build_edge_index_sorted(FILE *fp, uint32_t *next_page, CBMDumpEdge *edges,
                                        int edge_count, int *perm, edge_cell_fn cell_fn) {
    if (edge_count <= 0) {
        return write_index_btree(fp, next_page, NULL, NULL, 0);
    }
    if (!perm) {
        return 0;
    }
    uint8_t **idx_cells = (uint8_t **)malloc(edge_count * sizeof(uint8_t *));
    int *idx_lens = (int *)malloc(edge_count * sizeof(int));
    if (!idx_cells || !idx_lens) {
        free(perm);
        free(idx_cells);
        free(idx_lens);
        return 0;
    }
    for (int i = 0; i < edge_count; i++) {
        int si = perm[i];
        idx_cells[i] = cell_fn(&edges[si], &idx_lens[i]);
        if (!idx_cells[i]) {
            for (int j = 0; j < i; j++) {
                free(idx_cells[j]);
            }
            free(idx_cells);
            free(idx_lens);
            free(perm);
            return 0;
        }
    }
    free(perm);
    uint32_t root = write_index_btree(fp, next_page, idx_cells, idx_lens, edge_count);
    for (int i = 0; i < edge_count; i++) {
        free(idx_cells[i]);
    }
    free(idx_cells);
    free(idx_lens);
    return root;
}

/* Node column getter for index building. */
typedef const char *(*node_col_fn)(const CBMDumpNode *n);
static const char *ncol_label(const CBMDumpNode *n) {
    return n->label;
}
static const char *ncol_name(const CBMDumpNode *n) {
    return n->name;
}
static const char *ncol_file(const CBMDumpNode *n) {
    return n->file_path ? n->file_path : "";
}
static const char *ncol_qn(const CBMDumpNode *n) {
    return n->qualified_name;
}

/* Build a 2-text node index from a pre-sorted permutation. Returns root page or 0. */
static uint32_t build_node_index_sorted(FILE *fp, uint32_t *next_page, CBMDumpNode *nodes,
                                        int node_count, int *perm, node_col_fn col_fn) {
    if (node_count <= 0) {
        return write_index_btree(fp, next_page, NULL, NULL, 0);
    }
    if (!perm) {
        return 0;
    }
    uint8_t **idx_cells = (uint8_t **)malloc(node_count * sizeof(uint8_t *));
    int *idx_lens = (int *)malloc(node_count * sizeof(int));
    if (!idx_cells || !idx_lens) {
        free(perm);
        free(idx_cells);
        free(idx_lens);
        return 0;
    }
    for (int i = 0; i < node_count; i++) {
        int si = perm[i];
        idx_cells[i] = build_index_entry_2text_rowid(nodes[si].project, col_fn(&nodes[si]),
                                                     nodes[si].id, &idx_lens[i]);
        if (!idx_cells[i]) {
            for (int j = 0; j < i; j++) {
                free(idx_cells[j]);
            }
            free(idx_cells);
            free(idx_lens);
            free(perm);
            return 0;
        }
    }
    free(perm);
    uint32_t root = write_index_btree(fp, next_page, idx_cells, idx_lens, node_count);
    for (int i = 0; i < node_count; i++) {
        free(idx_cells[i]);
    }
    free(idx_cells);
    free(idx_lens);
    return root;
}

// --- Main entry point ---

/* Write context passed to sub-phases of cbm_write_db. */
typedef struct {
    FILE *fp;
    uint32_t next_page;
    const char *project;
    const char *root_path;
    const char *indexed_at;
    CBMDumpNode *nodes;
    int node_count;
    CBMDumpEdge *edges;
    int edge_count;
    CBMDumpVector *vectors;
    int vector_count;
    CBMDumpTokenVec *token_vecs;
    int token_vec_count;
} write_db_ctx_t;

/* Callback type for building a record from an item at index i. */
typedef uint8_t *(*build_record_fn)(const void *items, int i, int *out_len);
typedef int64_t (*get_rowid_fn)(const void *items, int i);

/* Write a streaming B-tree table from count items, or an empty table if count == 0. */
static int write_one_table(write_db_ctx_t *w, uint32_t *root, const void *items, int count,
                           build_record_fn build_rec, get_rowid_fn get_id) {
    if (count <= 0 || !items) {
        *root = write_table_btree(w->fp, &w->next_page, NULL, NULL, NULL, 0, false);
        return 0;
    }
    PageBuilder pb;
    pb_init(&pb, w->fp, w->next_page, false);
    for (int i = 0; i < count; i++) {
        int rec_len;
        uint8_t *rec = build_rec(items, i, &rec_len);
        if (!rec) {
            return ERR_WRITE_FAILED;
        }
        int64_t rowid = get_id(items, i);
        int64_t prev_id = i > 0 ? get_id(items, i - SKIP_ONE) : 0;
        pb_add_table_cell_with_flush(&pb, rowid, rec, rec_len, prev_id);
        free(rec);
    }
    *root = pb_finalize_table(&pb, &w->next_page, get_id(items, count - SKIP_ONE));
    return 0;
}

/* Adapter functions for write_one_table */
static uint8_t *adapt_build_node(const void *items, int i, int *out_len) {
    return build_node_record(&((const CBMDumpNode *)items)[i], out_len);
}
static int64_t adapt_node_id(const void *items, int i) {
    return ((const CBMDumpNode *)items)[i].id;
}
static uint8_t *adapt_build_edge(const void *items, int i, int *out_len) {
    return build_edge_record(&((const CBMDumpEdge *)items)[i], out_len);
}
static int64_t adapt_edge_id(const void *items, int i) {
    return ((const CBMDumpEdge *)items)[i].id;
}
static uint8_t *adapt_build_vector(const void *items, int i, int *out_len) {
    return build_vector_record(&((const CBMDumpVector *)items)[i], out_len);
}
static int64_t adapt_vector_id(const void *items, int i) {
    return ((const CBMDumpVector *)items)[i].node_id;
}
static uint8_t *adapt_build_token_vec(const void *items, int i, int *out_len) {
    return build_token_vec_record(&((const CBMDumpTokenVec *)items)[i], out_len);
}
static int64_t adapt_token_vec_id(const void *items, int i) {
    return ((const CBMDumpTokenVec *)items)[i].id;
}

/* Phase 1: Write node + edge + vector data tables (streaming). */
static int write_data_tables(write_db_ctx_t *w, uint32_t *nodes_root, uint32_t *edges_root,
                             uint32_t *vectors_root, uint32_t *token_vecs_root) {
    int rc;
    rc = write_one_table(w, nodes_root, w->nodes, w->node_count, adapt_build_node, adapt_node_id);
    if (rc != 0) {
        return rc;
    }
    rc = write_one_table(w, edges_root, w->edges, w->edge_count, adapt_build_edge, adapt_edge_id);
    if (rc != 0) {
        return rc;
    }
    rc = write_one_table(w, vectors_root, w->vectors, w->vector_count, adapt_build_vector,
                         adapt_vector_id);
    if (rc != 0) {
        return rc;
    }
    rc = write_one_table(w, token_vecs_root, w->token_vecs, w->token_vec_count,
                         adapt_build_token_vec, adapt_token_vec_id);
    return rc;
}

/* Phase 2: Write metadata tables (projects, file_hashes, summaries, sqlite_sequence). */
static void write_metadata_tables(write_db_ctx_t *w, uint32_t *projects_root,
                                  uint32_t *file_hashes_root, uint32_t *summaries_root,
                                  uint32_t *sqlite_seq_root) {
    int proj_rec_len;
    uint8_t *proj_rec =
        build_project_record(w->project, w->indexed_at, w->root_path, &proj_rec_len);
    const uint8_t *proj_recs[] = {proj_rec};
    int proj_lens[] = {proj_rec_len};
    int64_t proj_rowids[] = {FIRST_ROWID};
    *projects_root =
        write_table_btree(w->fp, &w->next_page, proj_recs, proj_lens, proj_rowids, SKIP_ONE, false);
    free(proj_rec);

    *file_hashes_root = write_table_btree(w->fp, &w->next_page, NULL, NULL, NULL, 0, false);
    *summaries_root = write_table_btree(w->fp, &w->next_page, NULL, NULL, NULL, 0, false);

    RecordBuilder r1;
    RecordBuilder r2;
    rec_init(&r1);
    rec_add_text(&r1, "nodes");
    rec_add_int(&r1, w->node_count > 0 ? w->nodes[w->node_count - SKIP_ONE].id : 0);
    int seq1_len;
    uint8_t *seq1 = rec_finalize(&r1, &seq1_len);
    rec_free(&r1);

    rec_init(&r2);
    rec_add_text(&r2, "edges");
    rec_add_int(&r2, w->edge_count > 0 ? w->edges[w->edge_count - SKIP_ONE].id : 0);
    int seq2_len;
    uint8_t *seq2 = rec_finalize(&r2, &seq2_len);
    rec_free(&r2);

    const uint8_t *seq_recs[] = {seq1, seq2};
    int seq_lens[] = {seq1_len, seq2_len};
    int64_t seq_rowids[] = {FIRST_ROWID, FIRST_DATA_PAGE};
    *sqlite_seq_root =
        write_table_btree(w->fp, &w->next_page, seq_recs, seq_lens, seq_rowids, PAIR_LEN, false);
    free(seq1);
    free(seq2);
}

/* Write the SQLite file header on page 1 with master entries. */
static void write_sqlite_file_header(uint8_t *page1, uint32_t total_pages) {
    memcpy(page1, "SQLite format 3\000", 16);
    put_u16(page1 + HDR_OFF_CBM_PAGE_SIZE,
            CBM_PAGE_SIZE == SQLITE_MAX_PAGE_SIZE ? (uint16_t)SKIP_ONE : (uint16_t)CBM_PAGE_SIZE);
    page1[HDR_OFF_WRITE_VERSION] = FILE_FORMAT;
    page1[HDR_OFF_READ_VERSION] = FILE_FORMAT;
    page1[HDR_OFF_RESERVED] = 0;
    page1[HDR_OFF_MAX_EMBED_FRAC] = MAX_EMBED_FRACTION;
    page1[HDR_OFF_MIN_EMBED_FRAC] = MIN_EMBED_FRACTION;
    page1[HDR_OFF_LEAF_FRAC] = LEAF_PAYLOAD_FRACTION;
    put_u32(page1 + HDR_OFF_FILE_CHANGE, SKIP_ONE);
    put_u32(page1 + HDR_OFF_DB_SIZE, total_pages);
    put_u32(page1 + HDR_OFF_FREELIST_TRUNK, 0);
    put_u32(page1 + HDR_OFF_FREELIST_COUNT, 0);
    put_u32(page1 + HDR_OFF_SCHEMA_COOKIE, SKIP_ONE);
    put_u32(page1 + HDR_OFF_SCHEMA_FORMAT, SCHEMA_FORMAT);
    put_u32(page1 + HDR_OFF_DEFAULT_CACHE, 0);
    put_u32(page1 + HDR_OFF_AUTOVAC_TOP, 0);
    put_u32(page1 + HDR_OFF_TEXT_ENCODING, SKIP_ONE);
    put_u32(page1 + HDR_OFF_USER_VERSION, 0);
    put_u32(page1 + HDR_OFF_INCR_VACUUM, 0);
    put_u32(page1 + HDR_OFF_APP_ID, 0);
    put_u32(page1 + HDR_OFF_VERSION_VALID, SKIP_ONE);
    put_u32(page1 + HDR_OFF_SQLITE_VERSION, SQLITE_VERSION);
}

/* Build master records, write page 1 B-tree + file header. */
static int write_master_page1(FILE *fp, MasterEntry *master, int master_count, uint32_t next_page) {
    const uint8_t **master_records = (const uint8_t **)malloc(master_count * sizeof(uint8_t *));
    int *master_lens = (int *)malloc(master_count * sizeof(int));
    int64_t *master_rowids = (int64_t *)malloc(master_count * sizeof(int64_t));
    for (int i = 0; i < master_count; i++) {
        master_rowids[i] = i + SKIP_ONE;
        master_records[i] = build_master_record(&master[i], &master_lens[i]);
    }

    uint8_t page1[CBM_PAGE_SIZE];
    memset(page1, 0, CBM_PAGE_SIZE);
    int hdr = SQLITE_HEADER_SIZE;
    page1[hdr] = BTREE_LEAF_TABLE;
    int content_off = CBM_PAGE_SIZE;
    int ptr_off = hdr + BTREE_HEADER_SIZE;
    int mcell_count = 0;

    for (int i = 0; i < master_count; i++) {
        int cell_len = 0;
        uint8_t *cell =
            build_table_cell(master_rowids[i], master_records[i], master_lens[i], &cell_len);
        int available = content_off - ptr_off - CELL_PTR_SIZE;
        if (!cell || cell_len > available) {
            free(cell);
            for (int j = 0; j < master_count; j++) {
                free((void *)master_records[j]);
            }
            free(master_records);
            free(master_lens);
            free(master_rowids);
            return ERR_MASTER_OVERFLOW;
        }
        content_off -= cell_len;
        memcpy(page1 + content_off, cell, cell_len);
        put_u16(page1 + ptr_off, (uint16_t)content_off);
        ptr_off += CELL_PTR_SIZE;
        mcell_count++;
        free(cell);
    }

    put_u16(page1 + hdr + HDR_FREEBLOCK_OFF, 0);
    put_u16(page1 + hdr + HDR_CELLCOUNT_OFF, (uint16_t)mcell_count);
    put_u16(page1 + hdr + HDR_CONTENT_OFF, (uint16_t)content_off);
    page1[hdr + HDR_FRAGBYTES_OFF] = 0;

    write_sqlite_file_header(page1, next_page - SKIP_ONE);

    (void)fseek(fp, 0, SEEK_SET);
    (void)fwrite(page1, SKIP_ONE, CBM_PAGE_SIZE, fp);

    for (int i = 0; i < master_count; i++) {
        free((void *)master_records[i]);
    }
    free(master_records);
    free(master_lens);
    free(master_rowids);
    return 0;
}

/* Pad file to exact page boundary. */
static void pad_file_to_page_boundary(FILE *fp, uint32_t next_page) {
    (void)fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    long expected_size = (long)(next_page - SKIP_ONE) * CBM_PAGE_SIZE;
    if (file_size < expected_size) {
        uint8_t zero = 0;
        (void)fseek(fp, expected_size - SKIP_ONE, SEEK_SET);
        (void)fwrite(&zero, SKIP_ONE, SKIP_ONE, fp);
    }
}

/* Build all 4 node index B-trees. Returns 0 on success, ERR_SORT_FAILED on failure. */
static int build_node_indexes(FILE *fp, uint32_t *next_page, CBMDumpNode *nodes, int node_count,
                              SortJob *nsorts, uint32_t *label_root, uint32_t *name_root,
                              uint32_t *file_root, uint32_t *qn_root) {
    *label_root =
        build_node_index_sorted(fp, next_page, nodes, node_count, nsorts[0].perm, ncol_label);
    *name_root = build_node_index_sorted(fp, next_page, nodes, node_count, nsorts[NSORT_NAME].perm,
                                         ncol_name);
    *file_root = build_node_index_sorted(fp, next_page, nodes, node_count, nsorts[NSORT_FILE].perm,
                                         ncol_file);
    *qn_root =
        build_node_index_sorted(fp, next_page, nodes, node_count, nsorts[NSORT_QN].perm, ncol_qn);
    if (node_count > 0 && (!*label_root || !*name_root || !*file_root || !*qn_root)) {
        return ERR_SORT_FAILED;
    }
    return 0;
}

/* Build all 7 edge index B-trees. Returns 0 on success, ERR_SORT_FAILED on failure. */
static int build_edge_indexes(FILE *fp, uint32_t *next_page, CBMDumpEdge *edges, int edge_count,
                              SortJob *esorts, uint32_t *source_root, uint32_t *target_root,
                              uint32_t *type_root, uint32_t *tgt_type_root, uint32_t *src_type_root,
                              uint32_t *url_path_root, uint32_t *auto_root) {
    *source_root =
        build_edge_index_sorted(fp, next_page, edges, edge_count, esorts[0].perm, ecell_source);
    *target_root = build_edge_index_sorted(fp, next_page, edges, edge_count,
                                           esorts[ESORT_TARGET].perm, ecell_target);
    *type_root = build_edge_index_sorted(fp, next_page, edges, edge_count, esorts[ESORT_TYPE].perm,
                                         ecell_type);
    *tgt_type_root = build_edge_index_sorted(
        fp, next_page, edges, edge_count, esorts[ESORT_PROJ_TGT_TYPE].perm, ecell_proj_target_type);
    *src_type_root = build_edge_index_sorted(
        fp, next_page, edges, edge_count, esorts[ESORT_PROJ_SRC_TYPE].perm, ecell_proj_source_type);
    *url_path_root = build_edge_index_sorted(fp, next_page, edges, edge_count,
                                             esorts[ESORT_URL_PATH].perm, ecell_url_path);
    *auto_root = build_edge_index_sorted(fp, next_page, edges, edge_count,
                                         esorts[ESORT_SRC_TGT_TYPE].perm, ecell_src_tgt_type);
    if (edge_count > 0 && (!*source_root || !*target_root || !*type_root || !*tgt_type_root ||
                           !*src_type_root || !*url_path_root || !*auto_root)) {
        return ERR_SORT_FAILED;
    }
    return 0;
}

/* Launch parallel sort threads for all index permutations. */
static void parallel_sort_indexes(SortJob *nsorts, int n_node, SortJob *esorts, int n_edge) {
    cbm_thread_t st[TOTAL_SORT_THREADS];
    int nt = 0;
    for (int i = 0; i < n_node; i++) {
        if (nsorts[i].count > 0) {
            cbm_thread_create(&st[nt++], 0, sort_worker, &nsorts[i]);
        }
    }
    for (int i = 0; i < n_edge; i++) {
        if (esorts[i].count > 0) {
            cbm_thread_create(&st[nt++], 0, sort_worker, &esorts[i]);
        }
    }
    for (int i = 0; i < nt; i++) {
        cbm_thread_join(&st[i]);
    }
}

int cbm_write_db(const char *path, const char *project, const char *root_path,
                 const char *indexed_at, CBMDumpNode *nodes, int node_count, CBMDumpEdge *edges,
                 int edge_count, CBMDumpVector *vectors, int vector_count,
                 CBMDumpTokenVec *token_vecs, int token_vec_count) {
    FILE *fp = fopen(path, "wb");
    if (!fp) {
        return CBM_NOT_FOUND;
    }

    write_db_ctx_t w = {.fp = fp,
                        .next_page = FIRST_DATA_PAGE,
                        .project = project,
                        .root_path = root_path,
                        .indexed_at = indexed_at,
                        .nodes = nodes,
                        .node_count = node_count,
                        .edges = edges,
                        .edge_count = edge_count,
                        .vectors = vectors,
                        .vector_count = vector_count,
                        .token_vecs = token_vecs,
                        .token_vec_count = token_vec_count};

    // Phase 1: Data tables (streaming node + edge + vector + token_vector records)
    CBM_PROF_START(t_data);
    uint32_t nodes_root;
    uint32_t edges_root;
    uint32_t vectors_root;
    uint32_t token_vecs_root;
    int rc = write_data_tables(&w, &nodes_root, &edges_root, &vectors_root, &token_vecs_root);
    if (rc != 0) {
        (void)fclose(fp);
        return rc;
    }
    CBM_PROF_END_N("write_db", "1_data_tables", t_data, node_count + edge_count);

    // Phase 2: Metadata tables (projects, file_hashes, summaries, sqlite_sequence)
    CBM_PROF_START(t_meta);
    uint32_t projects_root;
    uint32_t file_hashes_root;
    uint32_t summaries_root;
    uint32_t sqlite_seq_root;
    write_metadata_tables(&w, &projects_root, &file_hashes_root, &summaries_root, &sqlite_seq_root);
    uint32_t next_page = w.next_page;
    CBM_PROF_END("write_db", "2_metadata_tables", t_meta);

    // --- Build indexes (all sorted by key columns before writing) ---

    // Set sort contexts for qsort comparators.
    g_sort_nodes = nodes;
    g_sort_edges = edges;

    // Parallel sort: all 11 index permutations sorted simultaneously.
    // Sorting is O(N log N) per index — the dominant CPU cost in index building.
    // Cell building + B-tree writing remains serial (sequential page allocation).
    SortJob nsorts[] = {
        {node_count, cmp_node_by_label, NULL},
        {node_count, cmp_node_by_name, NULL},
        {node_count, cmp_node_by_file, NULL},
        {node_count, cmp_node_by_qn, NULL},
    };
    SortJob esorts[] = {
        {edge_count, cmp_edge_by_source_type, NULL},
        {edge_count, cmp_edge_by_target_type, NULL},
        {edge_count, cmp_edge_by_type, NULL},
        {edge_count, cmp_edge_by_proj_target_type, NULL},
        {edge_count, cmp_edge_by_proj_source_type, NULL},
        {edge_count, cmp_edge_by_url_path, NULL},
        {edge_count, cmp_edge_by_src_tgt_type, NULL},
    };

    CBM_PROF_START(t_sort);
    parallel_sort_indexes(nsorts, NODE_SORT_THREADS, esorts, EDGE_SORT_THREADS);
    CBM_PROF_END_N("write_db", "3_parallel_sort_indexes", t_sort, node_count + edge_count);

    /* Phase 4-5: Build node + edge index B-trees */
    CBM_PROF_START(t_node_idx);
    uint32_t idx_nodes_label_root;
    uint32_t idx_nodes_name_root;
    uint32_t idx_nodes_file_root;
    uint32_t autoindex_nodes_root;
    int nrc = build_node_indexes(fp, &next_page, nodes, node_count, nsorts, &idx_nodes_label_root,
                                 &idx_nodes_name_root, &idx_nodes_file_root, &autoindex_nodes_root);
    CBM_PROF_END_N("write_db", "4_node_indexes_seq", t_node_idx, node_count * NODE_SORT_THREADS);
    if (nrc != 0) {
        (void)fclose(fp);
        return nrc;
    }

    CBM_PROF_START(t_edge_idx);
    uint32_t idx_edges_source_root;
    uint32_t idx_edges_target_root;
    uint32_t idx_edges_type_root;
    uint32_t idx_edges_target_type_root;
    uint32_t idx_edges_source_type_root;
    uint32_t idx_edges_url_path_root;
    uint32_t autoindex_edges_root;
    int erc = build_edge_indexes(fp, &next_page, edges, edge_count, esorts, &idx_edges_source_root,
                                 &idx_edges_target_root, &idx_edges_type_root,
                                 &idx_edges_target_type_root, &idx_edges_source_type_root,
                                 &idx_edges_url_path_root, &autoindex_edges_root);
    CBM_PROF_END_N("write_db", "5_edge_indexes_seq", t_edge_idx, edge_count * EDGE_SORT_THREADS);
    if (erc != 0) {
        (void)fclose(fp);
        return erc;
    }

    // Autoindex for projects(name TEXT PK) — single text column
    uint32_t autoindex_projects_root;
    {
        // 1 row: project name
        RecordBuilder r;
        rec_init(&r);
        rec_add_text(&r, project);
        rec_add_int(&r, FIRST_ROWID); /* rowid */
        int plen = 0;
        uint8_t *payload = rec_finalize(&r, &plen);
        rec_free(&r);
        int vl = varint_len(plen);
        int total = vl + plen;
        uint8_t *cell = (uint8_t *)malloc(total);
        int pos = put_varint(cell, plen);
        memcpy(cell + pos, payload, plen);
        free(payload);
        uint8_t *cells_arr[] = {cell};
        int lens_arr[] = {total};
        autoindex_projects_root = write_index_btree(fp, &next_page, cells_arr, lens_arr, SKIP_ONE);
        free(cell);
    }

    // Autoindex for file_hashes(project, rel_path PK) — empty (0 rows)
    uint32_t autoindex_file_hashes_root = write_index_btree(fp, &next_page, NULL, NULL, 0);

    // Autoindex for project_summaries(project TEXT PK) — empty (0 rows)
    uint32_t autoindex_summaries_root = write_index_btree(fp, &next_page, NULL, NULL, 0);

    // --- sqlite_master table (page 1) ---
    // This must be written last because it references root pages of all other tables/indexes.

    // CRITICAL: sqlite_master entries must follow standard SQLite ordering:
    // table → autoindex → user indexes → next table → autoindex → user indexes → ...
    // SQLite's schema loader expects autoindexes immediately after their table.
    // Mis-ordering causes rootpage mapping corruption in the schema cache.
    MasterEntry master[] = {
        {"table", "projects", "projects", projects_root,
         "CREATE TABLE projects (\n\t\tname TEXT PRIMARY KEY,\n\t\tindexed_at TEXT NOT "
         "NULL,\n\t\troot_path TEXT NOT NULL\n\t)"},
        {"index", "sqlite_autoindex_projects_1", "projects", autoindex_projects_root, NULL},
        {"table", "file_hashes", "file_hashes", file_hashes_root,
         "CREATE TABLE file_hashes (\n\t\tproject TEXT NOT NULL REFERENCES projects(name) ON "
         "DELETE CASCADE,\n\t\trel_path TEXT NOT NULL,\n\t\tsha256 TEXT NOT NULL,\n\t\tmtime_ns "
         "INTEGER NOT NULL DEFAULT 0,\n\t\tsize INTEGER NOT NULL DEFAULT 0,\n\t\tPRIMARY KEY "
         "(project, rel_path)\n\t)"},
        {"index", "sqlite_autoindex_file_hashes_1", "file_hashes", autoindex_file_hashes_root,
         NULL},
        {"table", "nodes", "nodes", nodes_root,
         "CREATE TABLE nodes (\n\t\tid INTEGER PRIMARY KEY AUTOINCREMENT,\n\t\tproject TEXT NOT "
         "NULL REFERENCES projects(name) ON DELETE CASCADE,\n\t\tlabel TEXT NOT NULL,\n\t\tname "
         "TEXT NOT NULL,\n\t\tqualified_name TEXT NOT NULL,\n\t\tfile_path TEXT DEFAULT "
         "'',\n\t\tstart_line INTEGER DEFAULT 0,\n\t\tend_line INTEGER DEFAULT 0,\n\t\tproperties "
         "TEXT DEFAULT '{}',\n\t\tUNIQUE(project, qualified_name)\n\t)"},
        {"index", "sqlite_autoindex_nodes_1", "nodes", autoindex_nodes_root, NULL},
        {"index", "idx_nodes_label", "nodes", idx_nodes_label_root,
         "CREATE INDEX idx_nodes_label ON nodes(project, label)"},
        {"index", "idx_nodes_name", "nodes", idx_nodes_name_root,
         "CREATE INDEX idx_nodes_name ON nodes(project, name)"},
        {"index", "idx_nodes_file", "nodes", idx_nodes_file_root,
         "CREATE INDEX idx_nodes_file ON nodes(project, file_path)"},
        {"table", "edges", "edges", edges_root,
         "CREATE TABLE edges (\n\t\tid INTEGER PRIMARY KEY AUTOINCREMENT,\n\t\tproject TEXT NOT "
         "NULL REFERENCES projects(name) ON DELETE CASCADE,\n\t\tsource_id INTEGER NOT NULL "
         "REFERENCES nodes(id) ON DELETE CASCADE,\n\t\ttarget_id INTEGER NOT NULL REFERENCES "
         "nodes(id) ON DELETE CASCADE,\n\t\ttype TEXT NOT NULL,\n\t\tproperties TEXT DEFAULT "
         "'{}',\n\t\turl_path_gen TEXT GENERATED ALWAYS AS "
         "(json_extract(properties,'$.url_path')),\n\t\tUNIQUE(source_id, target_id, type)\n\t)"},
        {"index", "sqlite_autoindex_edges_1", "edges", autoindex_edges_root, NULL},
        {"index", "idx_edges_source", "edges", idx_edges_source_root,
         "CREATE INDEX idx_edges_source ON edges(source_id, type)"},
        {"index", "idx_edges_target", "edges", idx_edges_target_root,
         "CREATE INDEX idx_edges_target ON edges(target_id, type)"},
        {"index", "idx_edges_type", "edges", idx_edges_type_root,
         "CREATE INDEX idx_edges_type ON edges(project, type)"},
        {"index", "idx_edges_target_type", "edges", idx_edges_target_type_root,
         "CREATE INDEX idx_edges_target_type ON edges(project, target_id, type)"},
        {"index", "idx_edges_source_type", "edges", idx_edges_source_type_root,
         "CREATE INDEX idx_edges_source_type ON edges(project, source_id, type)"},
        {"index", "idx_edges_url_path", "edges", idx_edges_url_path_root,
         "CREATE INDEX idx_edges_url_path ON edges(project, url_path_gen)"},
        {"table", "project_summaries", "project_summaries", summaries_root,
         "CREATE TABLE project_summaries (\n\t\t\tproject TEXT PRIMARY KEY,\n\t\t\tsummary TEXT "
         "NOT NULL,\n\t\t\tsource_hash TEXT NOT NULL,\n\t\t\tcreated_at TEXT NOT "
         "NULL,\n\t\t\tupdated_at TEXT NOT NULL\n\t\t)"},
        {"index", "sqlite_autoindex_project_summaries_1", "project_summaries",
         autoindex_summaries_root, NULL},
        {"table", "node_vectors", "node_vectors", vectors_root,
         "CREATE TABLE node_vectors (\n\t\tnode_id INTEGER PRIMARY KEY,\n\t\tproject TEXT NOT "
         "NULL,\n\t\tvector BLOB NOT NULL\n\t)"},
        {"table", "token_vectors", "token_vectors", token_vecs_root,
         "CREATE TABLE token_vectors (\n\t\tid INTEGER PRIMARY KEY,\n\t\tproject "
         "TEXT NOT NULL,\n\t\ttoken TEXT NOT NULL,\n\t\tvector BLOB NOT NULL,\n\t\tidf INTEGER "
         "NOT NULL\n\t)"},
        {"table", "sqlite_sequence", "sqlite_sequence", sqlite_seq_root,
         "CREATE TABLE sqlite_sequence(name,seq)"},
    };

    int master_count = sizeof(master) / sizeof(master[0]);
    int rc2 = write_master_page1(fp, master, master_count, next_page);
    if (rc2 != 0) {
        (void)fclose(fp);
        return rc2;
    }
    pad_file_to_page_boundary(fp, next_page);
    (void)fclose(fp);
    return 0;
}
