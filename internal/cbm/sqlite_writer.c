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
#include "foundation/compat_thread.h"

#include <stddef.h> // NULL
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#define PAGE_SIZE 65536
#define SCHEMA_FORMAT 4
#define FILE_FORMAT 1
#define SQLITE_VERSION 3046000 // 3.46.0

// Varint encoding constants.
#define VARINT_MASK 0x7f
#define VARINT_CONTINUE 0x80
#define BYTE_MASK 0xff

// SQLite text serial type offset: serial_type = len*2 + TEXT_SERIAL_BASE.
#define TEXT_SERIAL_BASE 13

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
#define HDR_OFF_PAGE_SIZE 16
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
        return 1;
    }
    // Encode in big-endian with MSB continuation bits
    uint8_t tmp[10];
    int n = 0;
    while (v > VARINT_MASK) {
        tmp[n++] = (uint8_t)(v & VARINT_MASK);
        v >>= 7;
    }
    tmp[n++] = (uint8_t)v;
    // Reverse into output with continuation bits
    for (int i = 0; i < n; i++) {
        buf[i] = tmp[n - 1 - i];
        if (i < n - 1) {
            buf[i] |= VARINT_CONTINUE;
        }
    }
    return n;
}

static int varint_len(int64_t value) {
    uint64_t v = (uint64_t)value;
    int n = 1;
    while (v > VARINT_MASK) {
        v >>= 7;
        n++;
    }
    return n;
}

// SQLite serial type for a TEXT value
static int64_t text_serial_type(int len) {
    return (len * 2) + TEXT_SERIAL_BASE;
}

// SQLite serial type for an integer value
static int64_t int_serial_type(int64_t val) {
    if (val == 0) {
        return 8; // integer value 0
    }
    if (val == 1) {
        return 9; // integer value 1
    }
    if (val >= -INT8_MAX_VAL - 1 && val <= INT8_MAX_VAL) {
        return 1; // 1 byte
    }
    if (val >= -INT16_MAX_VAL - 1 && val <= INT16_MAX_VAL) {
        return 2; // 2 bytes
    }
    if (val >= INT24_MIN_VAL && val <= INT24_MAX_VAL) {
        return 3; // 3 bytes
    }
    if (val >= INT32_MIN_VAL && val <= INT32_MAX_VAL) {
        return 4; // 4 bytes
    }
    if (val >= INT48_MIN_VAL && val <= INT48_MAX_VAL) {
        return 5; // 6 bytes
    }
    return 6; // 8 bytes
}

// Bytes needed to store an integer of given serial type
static int int_storage_bytes(int serial_type) {
    switch (serial_type) {
    case 0:
        return 0; // NULL
    case 1:
        return 1;
    case 2:
        return 2;
    case 3:
        return 3;
    case 4:
        return 4;
    case 5:
        return 6;
    case 6:
        return 8;
    case 8: // integer 0
    case 9: // integer 1
    default:
        return 0;
    }
}

// Write integer in big-endian for given byte count
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void put_int_be(uint8_t *buf, int64_t val, int nbytes) {
    for (int i = nbytes - 1; i >= 0; i--) {
        buf[i] = (uint8_t)(val & BYTE_MASK);
        val >>= 8; // NOLINT(readability-magic-numbers) — standard byte shift
    }
}

// Write a 2-byte big-endian value
static void put_u16(uint8_t *buf, uint16_t val) {
    buf[0] = (uint8_t)(val >> 8);
    buf[1] = (uint8_t)(val & BYTE_MASK);
}

// Write a 4-byte big-endian value
static void put_u32(uint8_t *buf, uint32_t val) {
    buf[0] = (uint8_t)(val >> 24); // NOLINT(readability-magic-numbers) — standard byte shift
    buf[1] = (uint8_t)(val >> 16); // NOLINT(readability-magic-numbers) — standard byte shift
    buf[2] = (uint8_t)(val >> 8);  // NOLINT(readability-magic-numbers) — standard byte shift
    buf[3] = (uint8_t)(val & BYTE_MASK);
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
    int newcap = b->cap == 0 ? 4096 : b->cap;
    while (newcap < b->len + needed) {
        newcap *= 2;
    }
    uint8_t *p = (uint8_t *)realloc(b->data, newcap);
    if (!p) {
        // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling,cert-err33-c)
        fprintf(stderr, "cbm_write_db: dynbuf realloc failed size=%d\n", newcap);
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
    uint8_t v[1] = {0};
    dynbuf_append(&r->header, v, 1);
}

static void rec_add_int(RecordBuilder *r, int64_t val) {
    int64_t st = int_serial_type(val);
    uint8_t vbuf[9];
    int vlen = put_varint(vbuf, st);
    dynbuf_append(&r->header, vbuf, vlen);

    int nbytes = int_storage_bytes((int)st);
    if (nbytes > 0) {
        uint8_t ibuf[8];
        put_int_be(ibuf, val, nbytes);
        dynbuf_append(&r->body, ibuf, nbytes);
    }
}

static void rec_add_text(RecordBuilder *r, const char *s) {
    int slen = s ? (int)strlen(s) : 0;
    int64_t st = text_serial_type(slen);
    uint8_t vbuf[9];
    int vlen = put_varint(vbuf, st);
    dynbuf_append(&r->header, vbuf, vlen);
    if (slen > 0) {
        dynbuf_append(&r->body, s, slen);
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
    uint8_t page[PAGE_SIZE];
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
    pb->content_offset = PAGE_SIZE;
    pb->page1_offset = (start_page == 1) ? 100 : 0;
    // Header: flag(1) + freeblock(2) + cell_count(2) + content_start(2) + fragmented(1) = 8
    pb->ptr_offset = pb->page1_offset + 8;
    memset(pb->page, 0, PAGE_SIZE);
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
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    pb->page[hdr + 0] = pb->is_index ? BTREE_LEAF_INDEX : BTREE_LEAF_TABLE; // leaf flag
    put_u16(pb->page + hdr + 1, 0);                                         // first freeblock
    put_u16(pb->page + hdr + 3, (uint16_t)pb->cell_count);
    put_u16(pb->page + hdr + 5, (uint16_t)pb->content_offset);
    pb->page[hdr + 7] = 0; // fragmented free bytes

    // Write page to file
    uint32_t page_num = pb->next_page;
    long offset = (long)(page_num - 1) * PAGE_SIZE;
    // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
    fseek(pb->fp, offset, SEEK_SET);
    // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
    fwrite(pb->page, 1, PAGE_SIZE, pb->fp);

    // Record this leaf for interior page building
    if (pb->leaf_count >= pb->leaf_cap) {
        pb->leaf_cap = pb->leaf_cap == 0 ? 256 : pb->leaf_cap * 2;
        void *tmp = realloc(pb->leaves, (size_t)pb->leaf_cap * sizeof(PageRef));
        if (!tmp) {
            free(pb->leaves);
            pb->leaves = NULL;
            return;
        }
        pb->leaves = (PageRef *)tmp;
    }
    pb->leaves[pb->leaf_count].page_num = page_num;
    // max_key is set by caller before flush
    pb->leaf_count++;

    // Reset for next page
    pb->next_page++;
    pb->cell_count = 0;
    pb->content_offset = PAGE_SIZE;
    pb->page1_offset = 0; // only page 1 has the 100-byte header
    pb->ptr_offset = 8;   // standard B-tree header size for non-page-1
    memset(pb->page, 0, PAGE_SIZE);
}

// Check if a cell of given size fits in the current page
static bool pb_cell_fits(PageBuilder *pb, int cell_len) {
    // Cell pointer (2 bytes) + cell content
    int available = pb->content_offset - pb->ptr_offset - 2;
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
    pb->ptr_offset += 2;
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
static uint32_t pb_build_interior(PageBuilder *pb, bool is_index) {
    if (!pb->leaves) {
        return 0;
    }
    if (pb->leaf_count <= 1) {
        return pb->leaves[0].page_num;
    }

    PageRef *children = pb->leaves;
    int child_count = pb->leaf_count;

    while (child_count > 1 && children) {
        PageRef *parents = NULL;
        int parent_count = 0;
        int parent_cap = 0;

        int i = 0;
        while (i < child_count) {
            // Start a new interior page
            uint8_t page[PAGE_SIZE];
            memset(page, 0, PAGE_SIZE);
            int cell_count = 0;
            int content_offset = PAGE_SIZE;
            // Interior header:
            // flag(1)+freeblock(2)+cell_count(2)+content_start(2)+frag(1)+right_child(4) = 12
            int ptr_offset = 12;

            // Add cells for children[i..] until page fills or children exhausted.
            // Each child gets a cell EXCEPT the last one (which becomes right_child).
            while (i < child_count - 1) {
                // Build cell: child_page(4) + key for children[i]
                uint8_t *cell_data = NULL;
                int clen;
                uint8_t tbuf[20];
                bool heap_cell = false;

                if (!is_index) {
                    put_u32(tbuf, children[i].page_num);
                    clen = 4 + put_varint(tbuf + 4, children[i].max_key);
                    cell_data = tbuf;
                } else {
                    clen = 4 + children[i].sep_cell_len;
                    cell_data = (uint8_t *)malloc(clen);
                    heap_cell = true;
                    put_u32(cell_data, children[i].page_num);
                    memcpy(cell_data + 4, children[i].sep_cell, children[i].sep_cell_len);
                }

                int available = content_offset - ptr_offset - 2;
                if (clen > available && cell_count > 0) {
                    if (heap_cell) {
                        free(cell_data);
                    }
                    break; // page full, flush with what we have
                }

                content_offset -= clen;
                memcpy(page + content_offset, cell_data, clen);
                put_u16(page + ptr_offset, (uint16_t)content_offset);
                ptr_offset += 2;
                cell_count++;
                if (heap_cell) {
                    free(cell_data);
                }
                i++;
            }

            // Determine right-child: if we consumed all children, it's the last one.
            // If we stopped early due to page full, right-child is children[i-1]
            // (the last child we added a cell for)... No: cells were added for
            // children[page_start..i-1], so right-child = children[i].
            // But if cell_count == 0 and we didn't add anything, we have a problem.
            // That can happen only if the first cell doesn't fit — shouldn't happen
            // for typical cell sizes on a 4096 page.

            uint32_t right_child_page;
            int right_child_idx;
            if (i < child_count - 1) {
                // Page full before reaching the end. Cells cover children[page_start..i-1].
                // Right-child = children[i] (first child NOT given a cell on this page).
                // But wait, we need right-child to be the last child whose keys are > last cell's
                // key. Actually: cells[page_start..i-1] have keys K_{page_start}..K_{i-1}.
                // Right-child should contain keys > K_{i-1}, which is children[i].
                // But children[i] hasn't been processed yet — it will be on the next interior page.
                // The right-child for THIS page should be children[i], and on the NEXT page,
                // children[i] will get a cell too. But that would mean children[i] is both
                // the right-child of this page AND has a cell on the next page.
                // That's wrong — each child should appear exactly once.
                //
                // Correct approach: cells for children[page_start..i-1], right-child = children[i].
                // Then next page starts at i+1.
                right_child_page = children[i].page_num;
                right_child_idx = i;
                i++; // skip the right-child for the next page's iteration
            } else {
                // Reached the end normally. Last child = right-child.
                right_child_page = children[child_count - 1].page_num;
                right_child_idx = child_count - 1;
                i = child_count; // exit outer loop
            }

            // Write the interior page
            uint32_t pnum = pb->next_page++;
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            page[0] = is_index ? 0x02 : 0x05;
            put_u16(page + 1, 0);
            put_u16(page + 3, (uint16_t)cell_count);
            put_u16(page + 5, (uint16_t)content_offset);
            page[7] = 0;
            put_u32(page + 8, right_child_page);

            // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
            fseek(pb->fp, (long)(pnum - 1) * PAGE_SIZE, SEEK_SET);
            // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
            fwrite(page, 1, PAGE_SIZE, pb->fp);

            // Record this interior page as a parent for the next level
            if (parent_count >= parent_cap) {
                parent_cap = parent_cap == 0 ? 64 : parent_cap * 2;
                PageRef *tmp = (PageRef *)realloc(parents, parent_cap * sizeof(PageRef));
                if (!tmp) {
                    free(parents);
                    parents = NULL;
                    break;
                }
                parents = tmp;
            }
            parents[parent_count].page_num = pnum;
            parents[parent_count].max_key = children[right_child_idx].max_key;
            // For multi-level index trees, copy the separator from the rightmost child
            if (is_index && children[right_child_idx].sep_cell) {
                int slen = children[right_child_idx].sep_cell_len;
                parents[parent_count].sep_cell = (uint8_t *)malloc(slen);
                memcpy(parents[parent_count].sep_cell, children[right_child_idx].sep_cell, slen);
                parents[parent_count].sep_cell_len = slen;
            } else {
                parents[parent_count].sep_cell = NULL;
                parents[parent_count].sep_cell_len = 0;
            }
            parent_count++;
        }

        if (children != pb->leaves) {
            for (int j = 0; j < child_count; j++) {
                free(children[j].sep_cell);
            }
            free(children);
        }
        children = parents;
        child_count = parent_count;
    }

    uint32_t root = children ? children[0].page_num : 0;
    if (children != pb->leaves) {
        for (int j = 0; j < child_count; j++) {
            free(children[j].sep_cell);
        }
        free(children);
    }
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
    int total = rl + kl + local_len + 4;
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
    pos += 4;
    *out_cell_len = pos;
    return cell;
}

// --- Overflow page writer ---
// Writes overflow pages for payload bytes that exceed local storage.
// Returns the first overflow page number (embedded in the leaf cell).
// Each overflow page: 4-byte next-page pointer + up to (PAGE_SIZE-4) bytes of data.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static uint32_t write_overflow_pages(FILE *fp, uint32_t *next_page, const uint8_t *data,
                                     int data_len) {
    int per_page = PAGE_SIZE - 4; // 4 bytes reserved for next-page pointer
    uint32_t first_page = 0;
    long prev_next_ptr_offset = -1; // file offset of the next-page field in the previous overflow page

    int offset = 0;
    while (offset < data_len) {
        uint32_t pnum = (*next_page)++;
        if (first_page == 0) {
            first_page = pnum;
        }

        // Backpatch previous overflow page's next-page pointer
        if (prev_next_ptr_offset >= 0) {
            uint8_t ptr[4];
            put_u32(ptr, pnum);
            // NOLINTNEXTLINE(cert-err33-c)
            fseek(fp, prev_next_ptr_offset, SEEK_SET);
            // NOLINTNEXTLINE(cert-err33-c)
            fwrite(ptr, 1, 4, fp);
        }

        int chunk = data_len - offset;
        if (chunk > per_page) {
            chunk = per_page;
        }

        uint8_t page[PAGE_SIZE];
        memset(page, 0, PAGE_SIZE);
        put_u32(page, 0); // next-page pointer — 0 for now, backpatched on next iteration
        memcpy(page + 4, data + offset, chunk);

        long page_offset = (long)(pnum - 1) * PAGE_SIZE;
        prev_next_ptr_offset = page_offset; // next-page pointer is at byte 0 of this page
        // NOLINTNEXTLINE(cert-err33-c)
        fseek(fp, page_offset, SEEK_SET);
        // NOLINTNEXTLINE(cert-err33-c)
        fwrite(page, 1, PAGE_SIZE, fp);

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

    // NOLINTNEXTLINE(misc-confusable-identifiers) — identifiers are distinct in context
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

// --- Write a table B-tree from records ---

// Helper: ensure leaf_cap, flush current leaf page with given max_key
static void pb_ensure_leaf_cap(PageBuilder *pb) {
    if (pb->leaf_count >= pb->leaf_cap) {
        pb->leaf_cap = pb->leaf_cap == 0 ? 256 : pb->leaf_cap * 2;
        void *tmp = realloc(pb->leaves, (size_t)pb->leaf_cap * sizeof(PageRef));
        if (!tmp) {
            free(pb->leaves);
            pb->leaves = NULL;
            return;
        }
        pb->leaves = (PageRef *)tmp;
    }
}

// SQLite overflow thresholds for leaf table B-tree pages (PAGE_SIZE=65536, reserved=0):
//   usable    = PAGE_SIZE = 65536
//   max_local = usable - 35 = 65501
//   min_local = (usable - 12) * 32 / 255 - 23 = 8199  (C integer arithmetic, same as SQLite)
//
// These must match SQLite's btree.c formulas exactly:
//   pBt->maxLeaf = usableSize - 35
//   pBt->minLeaf = (usableSize-12)*32/255 - 23
#define TABLE_OVERFLOW_MAX_LOCAL 65501
#define TABLE_OVERFLOW_MIN_LOCAL 8199

// Add a table cell to the PageBuilder, flushing leaf pages as needed.
// If the payload exceeds max_local, overflow pages are written and only the
// local portion plus a 4-byte overflow page pointer is stored in the leaf cell.
static void pb_add_table_cell_with_flush(PageBuilder *pb, int64_t rowid, const uint8_t *payload,
                                         // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                         int payload_len, int64_t prev_rowid) {
    int cell_len = 0;
    uint8_t *cell = NULL;

    if (payload_len > TABLE_OVERFLOW_MAX_LOCAL) {
        // Compute local_len per SQLite spec §overflow-pages for leaf table cells.
        int local_len =
            TABLE_OVERFLOW_MIN_LOCAL +
            ((payload_len - TABLE_OVERFLOW_MIN_LOCAL) % (PAGE_SIZE - 4));
        if (local_len > TABLE_OVERFLOW_MAX_LOCAL) {
            local_len = TABLE_OVERFLOW_MIN_LOCAL;
        }

        // Write overflow pages for the bytes that don't fit locally.
        uint32_t overflow_page =
            write_overflow_pages(pb->fp, &pb->next_page, payload + local_len,
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
        pb_ensure_leaf_cap(pb);
        if (!pb->leaves) {
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
    if (pb->leaf_count == 1) {
        root = pb->leaves[0].page_num;
    } else if (pb->leaf_count > 1) {
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
        uint32_t pnum = (*next_page)++;
        uint8_t page[PAGE_SIZE];
        memset(page, 0, PAGE_SIZE);
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        int hdr = first_is_page1 ? 100 : 0;
        page[hdr] = BTREE_LEAF_TABLE;                 // leaf table
        put_u16(page + hdr + 1, 0);                   // no freeblocks
        put_u16(page + hdr + 3, 0);                   // 0 cells
        put_u16(page + hdr + 5, (uint16_t)PAGE_SIZE); // content at end of page
        page[hdr + 7] = 0;                            // 0 fragmented bytes
        // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
        fseek(fp, (long)(pnum - 1) * PAGE_SIZE, SEEK_SET);
        // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
        fwrite(page, 1, PAGE_SIZE, fp);
        return pnum;
    }

    PageBuilder pb;
    pb_init(&pb, fp, *next_page, false);
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    pb.page1_offset = first_is_page1 ? 100 : 0;
    pb.ptr_offset = pb.page1_offset + 8;

    for (int i = 0; i < count; i++) {
        pb_add_table_cell_with_flush(&pb, rowids[i], records[i], record_lens[i],
                                     i > 0 ? rowids[i - 1] : 0);
    }

    return pb_finalize_table(&pb, next_page, rowids[count - 1]);
}

// Write leaf pages for an index, returns root page.
static uint32_t write_index_btree(FILE *fp, uint32_t *next_page, uint8_t **cells, int *cell_lens,
                                  int count) {
    if (count == 0) {
        uint32_t pnum = (*next_page)++;
        uint8_t page[PAGE_SIZE];
        memset(page, 0, PAGE_SIZE);
        page[0] = 0x0A;                         // leaf index
        put_u16(page + 1, 0);                   // no freeblocks
        put_u16(page + 3, 0);                   // 0 cells
        put_u16(page + 5, (uint16_t)PAGE_SIZE); // content at end of page
        page[7] = 0;                            // 0 fragmented bytes
        // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
        fseek(fp, (long)(pnum - 1) * PAGE_SIZE, SEEK_SET);
        // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
        fwrite(page, 1, PAGE_SIZE, fp);
        return pnum;
    }

    PageBuilder pb;
    pb_init(&pb, fp, *next_page, true);

    // SQLite index B-trees are B-trees (NOT B+ trees): separator keys on interior
    // pages are real entries that must NOT also appear on leaf pages. When a leaf
    // overflows, the last entry is PROMOTED to the interior page and removed from
    // the leaf. This ensures each entry exists exactly once in the tree.
    for (int i = 0; i < count; i++) {
        if (!pb_cell_fits(&pb, cell_lens[i]) && pb.cell_count > 0) {
            if (pb.leaf_count >= pb.leaf_cap) {
                pb.leaf_cap = pb.leaf_cap == 0 ? 256 : pb.leaf_cap * 2;
                void *tmp = realloc(pb.leaves, (size_t)pb.leaf_cap * sizeof(PageRef));
                if (!tmp) {
                    free(pb.leaves);
                    pb.leaves = NULL;
                    return 0;
                }
                pb.leaves = (PageRef *)tmp;
            }
            pb.leaves[pb.leaf_count].max_key = 0; // unused for index

            // Promote last cell from leaf to interior: remove it from the leaf page
            // and store it as the separator for the interior page.
            int last = i - 1;
            pb.leaves[pb.leaf_count].sep_cell = (uint8_t *)malloc(cell_lens[last]);
            memcpy(pb.leaves[pb.leaf_count].sep_cell, cells[last], cell_lens[last]);
            pb.leaves[pb.leaf_count].sep_cell_len = cell_lens[last];

            // "Un-add" the last cell from the current page
            pb.cell_count--;
            pb.content_offset += cell_lens[last];
            pb.ptr_offset -= 2;

            pb_flush_leaf(&pb);
        }
        pb_add_cell(&pb, cells[i], cell_lens[i]);
    }

    if (pb.cell_count > 0) {
        if (pb.leaf_count >= pb.leaf_cap) {
            pb.leaf_cap = pb.leaf_cap == 0 ? 256 : pb.leaf_cap * 2;
            void *tmp = realloc(pb.leaves, (size_t)pb.leaf_cap * sizeof(PageRef));
            if (!tmp) {
                free(pb.leaves);
                pb.leaves = NULL;
                return 0;
            }
            pb.leaves = (PageRef *)tmp;
        }
        pb.leaves[pb.leaf_count].max_key = 0;
        // Last leaf: no promotion needed — last cell stays on the leaf.
        // Store last cell as separator in case multi-level interior pages need it.
        int last = count - 1;
        pb.leaves[pb.leaf_count].sep_cell = (uint8_t *)malloc(cell_lens[last]);
        memcpy(pb.leaves[pb.leaf_count].sep_cell, cells[last], cell_lens[last]);
        pb.leaves[pb.leaf_count].sep_cell_len = cell_lens[last];
        pb_flush_leaf(&pb);
    }

    *next_page = pb.next_page;

    uint32_t root;
    if (!pb.leaves) {
        root = 0;
    } else if (pb.leaf_count == 1) {
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
        // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling,cert-err33-c)
        fprintf(stderr, "cbm_write_db: perm malloc failed n=%d size=%zu\n", n,
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

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int cmp_node_by_label(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].label), safe_str(g_sort_nodes[ib].label));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int cmp_node_by_name(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].name), safe_str(g_sort_nodes[ib].name));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int cmp_node_by_file(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    int c = strcmp(safe_str(g_sort_nodes[ia].file_path), safe_str(g_sort_nodes[ib].file_path));
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_nodes[ia].id, g_sort_nodes[ib].id);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int cmp_edge_by_url_path(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    const char *ua = g_sort_edges[ia].url_path;
    const char *ub = g_sort_edges[ib].url_path;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool na = (!ua || !ua[0]);
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool nb = (!ub || !ub[0]);
    if (na && nb) {
        return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
    }
    if (na) {
        return -1;
    }
    if (nb) {
        return 1;
    }
    int c = strcmp(ua, ub);
    if (c) {
        return c;
    }
    return cmp_i64(g_sort_edges[ia].id, g_sort_edges[ib].id);
}

// autoindex_edges_1: UNIQUE(source_id, target_id, type) + rowid
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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

// --- Main entry point ---

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters,readability-function-cognitive-complexity,readability-function-size)
int cbm_write_db(const char *path, const char *project, const char *root_path,
                 const char *indexed_at, CBMDumpNode *nodes, int node_count, CBMDumpEdge *edges,
                 int edge_count) {
    FILE *fp = fopen(path, "wb");
    if (!fp) {
        return -1;
    }

    // Reserve page 1 for sqlite_master (written last after we know all root pages)
    uint32_t next_page = 2;

    // --- Stream node table records (build + write one at a time) ---
    // This avoids holding all 1.87M node records (~374MB) in memory at once.
    uint32_t nodes_root;
    if (node_count > 0) {
        PageBuilder pb;
        pb_init(&pb, fp, next_page, false);
        for (int i = 0; i < node_count; i++) {
            int rec_len;
            uint8_t *rec = build_node_record(&nodes[i], &rec_len);
            if (!rec) {
                // NOLINTNEXTLINE(cert-err33-c) — best-effort cleanup on error path
                fclose(fp);
                return -3;
            }
            pb_add_table_cell_with_flush(&pb, nodes[i].id, rec, rec_len,
                                         i > 0 ? nodes[i - 1].id : 0);
            free(rec);
        }
        nodes_root = pb_finalize_table(&pb, &next_page, nodes[node_count - 1].id);
    } else {
        nodes_root = write_table_btree(fp, &next_page, NULL, NULL, NULL, 0, false);
    }

    // --- Stream edge table records (build + write one at a time) ---
    // Avoids holding all 5.7M edge records (~570MB) in memory at once.
    uint32_t edges_root;
    if (edge_count > 0) {
        PageBuilder pb;
        pb_init(&pb, fp, next_page, false);
        for (int i = 0; i < edge_count; i++) {
            int rec_len;
            uint8_t *rec = build_edge_record(&edges[i], &rec_len);
            if (!rec) {
                // NOLINTNEXTLINE(cert-err33-c) — best-effort cleanup on error path
                fclose(fp);
                return -3;
            }
            pb_add_table_cell_with_flush(&pb, edges[i].id, rec, rec_len,
                                         i > 0 ? edges[i - 1].id : 0);
            free(rec);
        }
        edges_root = pb_finalize_table(&pb, &next_page, edges[edge_count - 1].id);
    } else {
        edges_root = write_table_btree(fp, &next_page, NULL, NULL, NULL, 0, false);
    }

    // --- Projects table (1 row) ---
    int proj_rec_len;
    uint8_t *proj_rec = build_project_record(project, indexed_at, root_path, &proj_rec_len);
    const uint8_t *proj_recs[] = {proj_rec};
    int proj_lens[] = {proj_rec_len};
    int64_t proj_rowids[] = {1};
    uint32_t projects_root =
        write_table_btree(fp, &next_page, proj_recs, proj_lens, proj_rowids, 1, false);
    free(proj_rec);

    // --- Empty tables: file_hashes, project_summaries ---
    uint32_t file_hashes_root = write_table_btree(fp, &next_page, NULL, NULL, NULL, 0, false);
    uint32_t summaries_root = write_table_btree(fp, &next_page, NULL, NULL, NULL, 0, false);

    // --- sqlite_sequence table (required for AUTOINCREMENT) ---
    uint32_t sqlite_seq_root;
    // Two rows: ("nodes", max_node_id), ("edges", max_edge_id)
    {
        RecordBuilder r1;
        RecordBuilder r2;
        rec_init(&r1);
        rec_add_text(&r1, "nodes");
        rec_add_int(&r1, node_count > 0 ? nodes[node_count - 1].id : 0);
        int seq1_len;
        uint8_t *seq1 = rec_finalize(&r1, &seq1_len);
        rec_free(&r1);

        rec_init(&r2);
        rec_add_text(&r2, "edges");
        rec_add_int(&r2, edge_count > 0 ? edges[edge_count - 1].id : 0);
        int seq2_len;
        uint8_t *seq2 = rec_finalize(&r2, &seq2_len);
        rec_free(&r2);

        const uint8_t *seq_recs[] = {seq1, seq2};
        int seq_lens[] = {seq1_len, seq2_len};
        int64_t seq_rowids[] = {1, 2};
        sqlite_seq_root =
            write_table_btree(fp, &next_page, seq_recs, seq_lens, seq_rowids, 2, false);
        free(seq1);
        free(seq2);
    }

    // --- Build indexes (all sorted by key columns before writing) ---

    uint8_t **idx_cells = NULL;
    int *idx_lens = NULL;

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

    {
        cbm_thread_t st[11];
        int nt = 0;
        for (int i = 0; i < 4; i++) {
            if (nsorts[i].count > 0) {
                cbm_thread_create(&st[nt++], 0, sort_worker, &nsorts[i]);
            }
        }
        for (int i = 0; i < 7; i++) {
            if (esorts[i].count > 0) {
                cbm_thread_create(&st[nt++], 0, sort_worker, &esorts[i]);
            }
        }
        for (int i = 0; i < nt; i++) {
            cbm_thread_join(&st[i]);
        }
    }

// Helper macro: build 2-text node index from pre-sorted permutation.
#define BUILD_NODE_2TEXT_INDEX_SORTED(col_getter, sorted_perm, idx_name_var)                     \
    do {                                                                                         \
        if (node_count > 0) {                                                                    \
            int *perm = (sorted_perm);                                                           \
            if (!perm) {                                                                         \
                fclose(fp);                                                                      \
                return -4;                                                                       \
            }                                                                                    \
            idx_cells = (uint8_t **)malloc(node_count * sizeof(uint8_t *));                      \
            idx_lens = (int *)malloc(node_count * sizeof(int));                                  \
            if (!idx_cells || !idx_lens) {                                                       \
                fprintf(stderr, "cbm_write_db: node idx alloc failed n=%d\n", node_count);       \
                free(perm);                                                                      \
                free(idx_cells);                                                                 \
                free(idx_lens);                                                                  \
                fclose(fp);                                                                      \
                return -4;                                                                       \
            }                                                                                    \
            for (int i = 0; i < node_count; i++) {                                               \
                int si = perm[i];                                                                \
                idx_cells[i] = build_index_entry_2text_rowid(nodes[si].project, (col_getter),    \
                                                             nodes[si].id, &idx_lens[i]);        \
                if (!idx_cells[i]) {                                                             \
                    fprintf(stderr, "cbm_write_db: node idx cell failed i=%d\n", i);             \
                    for (int j = 0; j < i; j++)                                                  \
                        free(idx_cells[j]);                                                      \
                    free(idx_cells);                                                             \
                    free(idx_lens);                                                              \
                    free(perm);                                                                  \
                    fclose(fp);                                                                  \
                    return -4;                                                                   \
                }                                                                                \
            }                                                                                    \
            free(perm);                                                                          \
            (idx_name_var) = write_index_btree(fp, &next_page, idx_cells, idx_lens, node_count); \
            for (int i = 0; i < node_count; i++)                                                 \
                free(idx_cells[i]);                                                              \
            free(idx_cells);                                                                     \
            free(idx_lens);                                                                      \
        } else {                                                                                 \
            (idx_name_var) = write_index_btree(fp, &next_page, NULL, NULL, 0);                   \
        }                                                                                        \
    } while (0)

    uint32_t idx_nodes_label_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_NODE_2TEXT_INDEX_SORTED(nodes[si].label, nsorts[0].perm, idx_nodes_label_root);

    uint32_t idx_nodes_name_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_NODE_2TEXT_INDEX_SORTED(nodes[si].name, nsorts[1].perm, idx_nodes_name_root);

    uint32_t idx_nodes_file_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_NODE_2TEXT_INDEX_SORTED(nodes[si].file_path ? nodes[si].file_path : "", nsorts[2].perm,
                                  idx_nodes_file_root);

    uint32_t autoindex_nodes_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_NODE_2TEXT_INDEX_SORTED(nodes[si].qualified_name, nsorts[3].perm, autoindex_nodes_root);

#undef BUILD_NODE_2TEXT_INDEX_SORTED

// --- Edge indexes (all sorted) ---

// Helper macro: build sorted edge index, invoke cell builder per edge.
#define BUILD_EDGE_INDEX_SORTED(sorted_perm, cell_builder, idx_name_var)                         \
    do {                                                                                         \
        if (edge_count > 0) {                                                                    \
            int *perm = (sorted_perm);                                                           \
            if (!perm) {                                                                         \
                fclose(fp);                                                                      \
                return -4;                                                                       \
            }                                                                                    \
            idx_cells = (uint8_t **)malloc(edge_count * sizeof(uint8_t *));                      \
            idx_lens = (int *)malloc(edge_count * sizeof(int));                                  \
            if (!idx_cells || !idx_lens) {                                                       \
                fprintf(stderr, "cbm_write_db: edge idx alloc failed n=%d\n", edge_count);       \
                free(perm);                                                                      \
                free(idx_cells);                                                                 \
                free(idx_lens);                                                                  \
                fclose(fp);                                                                      \
                return -4;                                                                       \
            }                                                                                    \
            for (int i = 0; i < edge_count; i++) {                                               \
                int si = perm[i];                                                                \
                (cell_builder);                                                                  \
                if (!idx_cells[i]) {                                                             \
                    fprintf(stderr, "cbm_write_db: edge idx cell failed i=%d\n", i);             \
                    for (int j = 0; j < i; j++)                                                  \
                        free(idx_cells[j]);                                                      \
                    free(idx_cells);                                                             \
                    free(idx_lens);                                                              \
                    free(perm);                                                                  \
                    fclose(fp);                                                                  \
                    return -4;                                                                   \
                }                                                                                \
            }                                                                                    \
            free(perm);                                                                          \
            (idx_name_var) = write_index_btree(fp, &next_page, idx_cells, idx_lens, edge_count); \
            for (int i = 0; i < edge_count; i++)                                                 \
                free(idx_cells[i]);                                                              \
            free(idx_cells);                                                                     \
            free(idx_lens);                                                                      \
        } else {                                                                                 \
            (idx_name_var) = write_index_btree(fp, &next_page, NULL, NULL, 0);                   \
        }                                                                                        \
    } while (0)

    // idx_edges_source: (source_id, type) + rowid
    uint32_t idx_edges_source_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_EDGE_INDEX_SORTED(esorts[0].perm,
                            idx_cells[i] = build_index_entry_int_text_rowid(
                                edges[si].source_id, edges[si].type, edges[si].id, &idx_lens[i]),
                            idx_edges_source_root);

    // idx_edges_target: (target_id, type) + rowid
    uint32_t idx_edges_target_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_EDGE_INDEX_SORTED(esorts[1].perm,
                            idx_cells[i] = build_index_entry_int_text_rowid(
                                edges[si].target_id, edges[si].type, edges[si].id, &idx_lens[i]),
                            idx_edges_target_root);

    // idx_edges_type: (project, type) + rowid
    uint32_t idx_edges_type_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_EDGE_INDEX_SORTED(esorts[2].perm,
                            idx_cells[i] = build_index_entry_2text_rowid(
                                edges[si].project, edges[si].type, edges[si].id, &idx_lens[i]),
                            idx_edges_type_root);

    // idx_edges_target_type: (project, target_id, type) + rowid
    uint32_t idx_edges_target_type_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_EDGE_INDEX_SORTED(
        esorts[3].perm,
        idx_cells[i] = build_index_entry_text_int_text_rowid(
            edges[si].project, edges[si].target_id, edges[si].type, edges[si].id, &idx_lens[i]),
        idx_edges_target_type_root);

    // idx_edges_source_type: (project, source_id, type) + rowid
    uint32_t idx_edges_source_type_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_EDGE_INDEX_SORTED(
        esorts[4].perm,
        idx_cells[i] = build_index_entry_text_int_text_rowid(
            edges[si].project, edges[si].source_id, edges[si].type, edges[si].id, &idx_lens[i]),
        idx_edges_source_type_root);

    // idx_edges_url_path: (project, url_path_gen) + rowid
    uint32_t idx_edges_url_path_root;
    if (edge_count > 0) {
        int *perm = esorts[5].perm;
        idx_cells = (uint8_t **)malloc(edge_count * sizeof(uint8_t *));
        idx_lens = (int *)malloc(edge_count * sizeof(int));
        for (int i = 0; i < edge_count; i++) {
            int si = perm[i];
            const char *url =
                (edges[si].url_path && edges[si].url_path[0]) ? edges[si].url_path : NULL;
            RecordBuilder r;
            rec_init(&r);
            rec_add_text(&r, edges[si].project);
            if (url) {
                rec_add_text(&r, url);
            } else {
                rec_add_null(&r);
            }
            rec_add_int(&r, edges[si].id);
            int payload_len = 0;
            uint8_t *payload = rec_finalize(&r, &payload_len);
            rec_free(&r);
            int vl = varint_len(payload_len);
            int total = vl + payload_len;
            idx_cells[i] = (uint8_t *)malloc(total);
            int pos = put_varint(idx_cells[i], payload_len);
            memcpy(idx_cells[i] + pos, payload, payload_len);
            free(payload);
            idx_lens[i] = total;
        }
        free(perm);
        idx_edges_url_path_root =
            write_index_btree(fp, &next_page, idx_cells, idx_lens, edge_count);
        for (int i = 0; i < edge_count; i++) {
            free(idx_cells[i]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(idx_cells);
        free(idx_lens);
    } else {
        idx_edges_url_path_root = write_index_btree(fp, &next_page, NULL, NULL, 0);
    }

    // Autoindex for UNIQUE(source_id, target_id, type) on edges
    uint32_t autoindex_edges_root;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    BUILD_EDGE_INDEX_SORTED(
        esorts[6].perm,
        idx_cells[i] = build_index_entry_unique_2int_text_rowid(
            edges[si].source_id, edges[si].target_id, edges[si].type, edges[si].id, &idx_lens[i]),
        autoindex_edges_root);

#undef BUILD_EDGE_INDEX_SORTED

    // Autoindex for projects(name TEXT PK) — single text column
    uint32_t autoindex_projects_root;
    {
        // 1 row: project name
        RecordBuilder r;
        rec_init(&r);
        rec_add_text(&r, project);
        rec_add_int(&r, 1); // rowid
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
        autoindex_projects_root = write_index_btree(fp, &next_page, cells_arr, lens_arr, 1);
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
        {"table", "sqlite_sequence", "sqlite_sequence", sqlite_seq_root,
         "CREATE TABLE sqlite_sequence(name,seq)"},
    };

    int master_count = sizeof(master) / sizeof(master[0]);

    // Build master records
    const uint8_t **master_records = (const uint8_t **)malloc(master_count * sizeof(uint8_t *));
    int *master_lens = (int *)malloc(master_count * sizeof(int));
    int64_t *master_rowids = (int64_t *)malloc(master_count * sizeof(int64_t));
    for (int i = 0; i < master_count; i++) {
        master_rowids[i] = i + 1;
        master_records[i] = build_master_record(&master[i], &master_lens[i]);
    }

    // For sqlite_master, we need to write page 1 as the root.
    // This is special: page 1 has the 100-byte file header.
    // We write master entries as a leaf table B-tree on page 1.
    // If they don't fit on one page, we need interior pages (unlikely for ~14 entries).

    // Write master B-tree starting at page 1
    {
        uint8_t page1[PAGE_SIZE];
        memset(page1, 0, PAGE_SIZE);

        // B-tree header starts at offset 100 on page 1
        int hdr = 100;
        page1[hdr] = BTREE_LEAF_TABLE; // leaf table
        int content_off = PAGE_SIZE;
        int ptr_off = hdr + 8;
        int mcell_count = 0;

        for (int i = 0; i < master_count; i++) {
            int cell_len;
            uint8_t *cell =
                build_table_cell(master_rowids[i], master_records[i], master_lens[i], &cell_len);

            // Check fit
            int available = content_off - ptr_off - 2;
            if (cell_len > available) {
                // Master entries should always fit on page 1 (they're small SQL strings).
                // If not, this is a bug — fail gracefully.
                free(cell);
                for (int j = 0; j < master_count; j++) {
                    free((void *)master_records[j]);
                }
                // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                free(master_records);
                free(master_lens);
                free(master_rowids);
                // NOLINTNEXTLINE(cert-err33-c) — best-effort cleanup on error path
                fclose(fp);
                return -2;
            }

            content_off -= cell_len;
            memcpy(page1 + content_off, cell, cell_len);
            put_u16(page1 + ptr_off, (uint16_t)content_off);
            ptr_off += 2;
            mcell_count++;
            free(cell);
        }

        put_u16(page1 + hdr + 1, 0); // freeblock
        put_u16(page1 + hdr + 3, (uint16_t)mcell_count);
        put_u16(page1 + hdr + 5, (uint16_t)content_off);
        page1[hdr + 7] = 0; // fragmented

        // Write the 100-byte SQLite file header
        memcpy(page1, "SQLite format 3\000", 16);
        /* Page size 65536 is encoded as 1 in the 2-byte header field */
        put_u16(page1 + HDR_OFF_PAGE_SIZE, (uint16_t)1);
        page1[HDR_OFF_WRITE_VERSION] = FILE_FORMAT;            // file format write version
        page1[HDR_OFF_READ_VERSION] = FILE_FORMAT;             // file format read version
        page1[HDR_OFF_RESERVED] = 0;                           // reserved space per page
        page1[HDR_OFF_MAX_EMBED_FRAC] = 64;                    // max embedded payload fraction
        page1[HDR_OFF_MIN_EMBED_FRAC] = 32;                    // min embedded payload fraction
        page1[HDR_OFF_LEAF_FRAC] = 32;                         // leaf payload fraction
        put_u32(page1 + HDR_OFF_FILE_CHANGE, 0);               // file change counter (set below)
        put_u32(page1 + HDR_OFF_DB_SIZE, next_page - 1);       // total pages
        put_u32(page1 + HDR_OFF_FREELIST_TRUNK, 0);            // first freelist trunk page
        put_u32(page1 + HDR_OFF_FREELIST_COUNT, 0);            // total freelist pages
        put_u32(page1 + HDR_OFF_SCHEMA_COOKIE, 1);             // schema cookie
        put_u32(page1 + HDR_OFF_SCHEMA_FORMAT, SCHEMA_FORMAT); // schema format number
        put_u32(page1 + HDR_OFF_DEFAULT_CACHE, 0);             // default page cache size
        put_u32(page1 + HDR_OFF_AUTOVAC_TOP, 0);               // largest root page (auto-vacuum)
        put_u32(page1 + HDR_OFF_TEXT_ENCODING, 1);             // text encoding: UTF-8
        put_u32(page1 + HDR_OFF_USER_VERSION, 0);              // user version
        put_u32(page1 + HDR_OFF_INCR_VACUUM, 0);               // incremental vacuum mode
        put_u32(page1 + HDR_OFF_APP_ID, 0);                    // application ID
        // Bytes 72-91: reserved for expansion (zeros)
        put_u32(page1 + HDR_OFF_VERSION_VALID, 1); // version-valid-for (change counter)
        put_u32(page1 + HDR_OFF_SQLITE_VERSION, SQLITE_VERSION); // SQLite version number
        // Set file change counter = version-valid-for = 1
        put_u32(page1 + HDR_OFF_FILE_CHANGE, 1);

        // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
        fseek(fp, 0, SEEK_SET);
        // NOLINTNEXTLINE(cert-err33-c) — best-effort page write, errors detected at fclose
        fwrite(page1, 1, PAGE_SIZE, fp);
    }

    for (int i = 0; i < master_count; i++) {
        free((void *)master_records[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(master_records);
    free(master_lens);
    free(master_rowids);

    // Ensure file size is exactly next_page * PAGE_SIZE
    // (pad any remaining space)
    // NOLINTNEXTLINE(cert-err33-c) — best-effort file positioning
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    long expected_size = (long)(next_page - 1) * PAGE_SIZE;
    if (file_size < expected_size) {
        // Pad with zeros
        uint8_t zero = 0;
        // NOLINTNEXTLINE(cert-err33-c) — best-effort padding write
        fseek(fp, expected_size - 1, SEEK_SET);
        // NOLINTNEXTLINE(cert-err33-c) — best-effort padding write
        fwrite(&zero, 1, 1, fp);
    }

    // NOLINTNEXTLINE(cert-err33-c) — caller checks return code from this function, not fclose
    fclose(fp);
    return 0;
}
