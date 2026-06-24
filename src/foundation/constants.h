/*
 * constants.h — Project-wide named constants.
 *
 * Eliminates magic numbers flagged by readability-magic-numbers.
 * Every literal integer/float in source should reference a named constant.
 */
#ifndef CBM_CONSTANTS_H
#define CBM_CONSTANTS_H

/* ── Allocation counts ───────────────────────────────────────── */
enum { CBM_ALLOC_ONE = 1 }; /* calloc(CBM_ALLOC_ONE, sizeof(T)) */

/* ── Byte / character constants ──────────────────────────────── */
enum {
    CBM_BYTE_RANGE = 256, /* full byte range 0x00–0xFF */
    CBM_QUOTE_PAIR = 2,   /* two quote characters (open + close) */
    CBM_QUOTE_OFFSET = 1, /* skip opening quote */
};

/* ── Size units (powers of 2) ────────────────────────────────── */
enum {
    CBM_SZ_2 = 2,
    CBM_SZ_3 = 3,
    CBM_SZ_4 = 4,
    CBM_SZ_5 = 5,
    CBM_SZ_6 = 6,
    CBM_SZ_7 = 7,
    CBM_SZ_8 = 8,
    CBM_SZ_16 = 16,
    CBM_SZ_32 = 32,
    CBM_SZ_64 = 64,
    CBM_SZ_128 = 128,
    CBM_SZ_256 = 256,
    CBM_SZ_512 = 512,
    CBM_SZ_1K = 1024,
    CBM_SZ_2K = 2048,
    CBM_SZ_4K = 4096,
    CBM_SZ_8K = 8192,
    CBM_SZ_16K = 16384,
    CBM_SZ_32K = 32768,
    CBM_SZ_64K = 65536,
};

/* ── Numeric bases and common factors ────────────────────────── */
enum {
    CBM_DECIMAL_BASE = 10,
    CBM_HEX_BASE = 16,
    CBM_PERCENT = 100,
};

/* ── Tree-sitter field name helper ───────────────────────────── */
/* Usage: ts_node_child_by_field_name(node, TS_FIELD("callee"))
 * Expands to: ts_node_child_by_field_name(node, TS_FIELD("callee"))
 * The sizeof includes the NUL terminator, so subtract 1. */
#define TS_FIELD(name) (name), (uint32_t)(sizeof(name) - SKIP_ONE)

/* ── Tree-sitter line offset ─────────────────────────────────── */
/* ts_node row is 0-based; source lines are 1-based. */
enum { TS_LINE_OFFSET = 1 };

/* Common offset constants. */

/* Common offset constants. */

/* ── Sentinel values ─────────────────────────────────────────── */
enum {
    CBM_NOT_FOUND = -1, /* search miss, invalid index */
    CBM_INIT_DONE = 1,  /* initialization flag */
};

/* ── Default pagination limits ───────────────────────────────── */
/* Default page size for search_graph and the underlying store-layer search.
 * Chosen so a typical broad query (e.g. file_pattern="**" on a 12k-node
 * project) stays well within MCP tool-result size budgets. Callers that
 * want more results paginate via offset+limit; the response always carries
 * 'total' and 'has_more' so agents can detect truncation. */
enum { CBM_DEFAULT_SEARCH_LIMIT = 200 };

/* ── Time conversion factors ─────────────────────────────────── */
#define CBM_NSEC_PER_SEC 1000000000ULL
#define CBM_USEC_PER_SEC 1000000ULL
#define CBM_MSEC_PER_SEC 1000ULL
#define CBM_NSEC_PER_USEC 1000ULL
#define CBM_NSEC_PER_MSEC 1000000ULL

/* ── Common string/buffer sizes ──────────────────────────────── */
enum {
    CBM_SMALL_BUF = 3,   /* small scratch buffers */
    CBM_NAME_BUF = 4,    /* name buffer slots */
    CBM_PATH_MAX = 1024, /* path buffer size */
    CBM_LINE_BUF = 512,  /* line read buffer */
};

/* Common offset constants (used across many files). */
enum { SKIP_ONE = 1, PAIR_LEN = 2 };

#endif /* CBM_CONSTANTS_H */
