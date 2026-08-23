/*
 * constants.h — Project-wide named constants.
 *
 * Eliminates magic numbers flagged by readability-magic-numbers.
 * Every literal integer/float in source should reference a named constant.
 */
#ifndef CBM_CONSTANTS_H
#define CBM_CONSTANTS_H

/* Expand a macro value before converting it to a string literal. */
#define CBM_STRINGIFY_INNER(value) #value
#define CBM_STRINGIFY(value) CBM_STRINGIFY_INNER(value)

/* Local builds without a release version use this non-semver sentinel. */
#define CBM_VERSION_DEVELOPMENT "dev"

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
 * Responses land in an LLM agent's context window, so the default favors a
 * cheap first page (~50 TOON rows ≈ 1.5K tokens) over raw coverage; the
 * response always carries 'total' and 'has_more', and agents page via
 * offset+limit or narrow with label/file_pattern when has_more is true. */
enum { CBM_DEFAULT_SEARCH_LIMIT = 50 };
/* String twin for compile-time concatenation into user-visible text (the
 * installed skill quotes this default), mirroring the
 * CBM_DEFAULT_QUERY_MAX_ROWS / _STR pair in cli.h. Published text must be built
 * from the value it describes, never restate it: a restated number stays
 * plausible and wrong after the value moves. The two spellings are locked
 * together by cli_installed_skill_limits_match_server_contract, which asserts
 * atoi(CBM_DEFAULT_SEARCH_LIMIT_STR) == CBM_DEFAULT_SEARCH_LIMIT. */
#define CBM_DEFAULT_SEARCH_LIMIT_STR "50"

/* ── Cypher query row budgets ────────────────────────────────── */
/* max_rows is an output-shaping cap. The working-row budget is a separate
 * correctness-preserving resource bound for intermediate bindings. An
 * explicit output cap raises the effective working budget to at least the
 * requested output size; both remain bounded by these operator-approved
 * maxima. Keep registry/help strings derived with CBM_STRINGIFY. */
#define CBM_DEFAULT_QUERY_MAX_ROWS 100000
#define CBM_MAX_QUERY_ROWS 1000000
#define CBM_DEFAULT_QUERY_MAX_WORKING_ROWS 100000
#define CBM_MAX_QUERY_WORKING_ROWS 1000000
#define CBM_DEFAULT_QUERY_MAX_ROWS_STR CBM_STRINGIFY(CBM_DEFAULT_QUERY_MAX_ROWS)
#define CBM_DEFAULT_QUERY_MAX_WORKING_ROWS_STR CBM_STRINGIFY(CBM_DEFAULT_QUERY_MAX_WORKING_ROWS)

/* ── Architecture defaults and working budgets ──────────────── */
/* User-visible architecture defaults are shared by config/help, MCP request
 * handling, and the store fallback. Keep string twins for registry text. */
#define CBM_DEFAULT_ARCH_HOTSPOT_LIMIT 25
#define CBM_DEFAULT_ARCH_HOTSPOT_LIMIT_STR CBM_STRINGIFY(CBM_DEFAULT_ARCH_HOTSPOT_LIMIT)
#define CBM_DEFAULT_ARCH_RESOLUTION 1.0
#define CBM_DEFAULT_ARCH_RESOLUTION_STR "1.0"

/* Leiden community detection has graph-sized runtime and memory cost. Keep a
 * conservative default until cross-parent benchmarks justify changing it, but
 * let operators select a much wider deliberate budget. Exhaustion must omit
 * clusters with explicit metadata; it must never publish a node-prefix result
 * as if it represented the complete graph. */
#define CBM_DEFAULT_ARCH_CLUSTER_NODE_BUDGET 8000
#define CBM_MIN_ARCH_CLUSTER_NODE_BUDGET 2
#define CBM_MAX_ARCH_CLUSTER_NODE_BUDGET 1000000
#define CBM_DEFAULT_ARCH_CLUSTER_NODE_BUDGET_STR CBM_STRINGIFY(CBM_DEFAULT_ARCH_CLUSTER_NODE_BUDGET)
#define CBM_MIN_ARCH_CLUSTER_NODE_BUDGET_STR CBM_STRINGIFY(CBM_MIN_ARCH_CLUSTER_NODE_BUDGET)

/* Resolution scoring: prefer production symbols over test/mock definitions
 * before namespace-distance tie-breaks. Shared by call and import resolvers so
 * duplicate-name behavior stays consistent. */
enum { CBM_RESOLUTION_NON_TEST_BONUS = 1000 };

/* ── Time conversion factors ─────────────────────────────────── */
#define CBM_NSEC_PER_SEC 1000000000ULL
#define CBM_USEC_PER_SEC 1000000ULL
#define CBM_MSEC_PER_SEC 1000ULL
#define CBM_USEC_PER_MSEC (CBM_USEC_PER_SEC / CBM_MSEC_PER_SEC)
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

/* ── Label allowlists for SQL ────────────────────────────────────
 * SQL mirror of cbm_label_is_type_like() (internal/cbm/helpers.c). That
 * function is documented as the single source of truth for type-like labels
 * "instead of scattering `|| strcmp(label,\"Struct\")==0` across the tree" —
 * but a SQL string literal cannot call it, so several queries hardcoded
 * ('Function','Method','Class') and silently stopped matching the moment
 * Struct/Interface/Enum/Type/Trait began being emitted.
 *
 * Use these instead of inlining a label list. tests/test_store_nodes.c pins
 * them against cbm_label_is_type_like(), so adding a type-like label there
 * without updating these fails CI rather than quietly shrinking query results. */
#define CBM_SQL_TYPE_LIKE_LABELS "'Class','Struct','Interface','Enum','Type','Trait'"
#define CBM_SQL_CALLABLE_LABELS "'Function','Method'"
#define CBM_SQL_CALLABLE_OR_TYPE_LABELS CBM_SQL_CALLABLE_LABELS "," CBM_SQL_TYPE_LIKE_LABELS
/* SQL mirror of cbm_label_is_relation() (Table/View/Model — data-lineage
 * nodes), pinned by tests/test_store_nodes.c the same way as the sets above. */
#define CBM_SQL_RELATION_LABELS "'Table','View','Model'"

/* ── Synthetic definition paths ──────────────────────────────────
 * Language layers mint reference-resolution stubs (Python/Kotlin builtins)
 * as real graph nodes so LSP-resolved CALLS/USAGE edges have a target. Their
 * file_path is an angle-bracket sentinel that cannot exist on disk; that
 * sentinel is the single contract marking a definition as synthetic. Every
 * minting site must use one of these constants, and every consumer that
 * needs "real project code only" must use the shared SQL fragment instead
 * of inventing its own string match.
 *
 * Rank computation excludes sentinel-path nodes: they are resolution
 * artifacts with universal fan-in, so left in they dominate every
 * rank-ordered surface in every project (flask canary: builtins.list and
 * builtins.str above Flask itself). Search returns them via LEFT JOIN; a
 * NULL rank orders after every ranked project symbol under ORDER BY DESC. */
#define CBM_SYNTHETIC_DEF_PATH_PY_BUILTINS "<python-builtins>"
#define CBM_SYNTHETIC_DEF_PATH_KT_BUILTINS "<kotlin-builtins>"
/* NULL file_path (Project/Package/Folder nodes) is not synthetic: only the
 * angle-bracket sentinel is. Real repository paths never start with '<'. */
#define CBM_SQL_EXCLUDE_SYNTHETIC_DEFS " AND (file_path IS NULL OR file_path NOT LIKE '<%')"

#endif /* CBM_CONSTANTS_H */
