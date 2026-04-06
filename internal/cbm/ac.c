// ac.c — Aho-Corasick multi-pattern matcher with fused LZ4 decompression.
//
// Custom implementation (~300 lines) because no permissive-licensed C AC
// libraries exist (ACISM and MultiFast are LGPL).
//
// Key design: pre-computed goto table (ACISM matrix approach) — for each
// (state, byte) pair the next state is a direct array lookup. Zero branches
// during scanning. Bitmask output for ≤64 patterns.

#include <stddef.h> // NULL
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "ac.h"
#include "foundation/compat.h"
#include "lz4_store.h"

// ─── Data structures ───────────────────────────────────────────────────────

// Maximum pattern count for bitmask mode.
#define CBM_AC_MAX_BITMASK 64

#define CBM_AC_BYTE_RANGE 256
#define CBM_AC_NO_STATE (-1)
#define CBM_AC_ROOT_STATES 1
#define CBM_AC_ALLOC_ONE 1
#define CBM_AC_PATTERN_BIT(p) (1ULL << (p))
#define CBM_AC_CLEAR_LOW_BIT(b) ((b) & ((b) - 1ULL))

// Decompression buffer alignment mask (round up to 64KB chunks).
#define DECOMP_BUF_ALIGN_MASK 0xFFFF

struct CBMAutomaton {
    int num_states;
    int num_patterns;
    int alpha_size;                       // 256 for raw byte, or smaller for mapped alphabet
    uint8_t alpha_map[CBM_AC_BYTE_RANGE]; // byte → mapped index (identity if alpha_size==256)
    int *go_table;                        // [num_states * alpha_size] — pre-computed transitions
    uint64_t *output;                     // [num_states] — bitmask of matching pattern IDs
    int *output_list;                     // [num_states] — linked list: pattern ID or -1
    int *output_next;                     // [num_states] — next pointer for output_list chain
};

// ─── Build ─────────────────────────────────────────────────────────────────

// Queue for BFS during failure function computation.
typedef struct {
    int *data;
    int head, tail, cap;
} Queue;

static void queue_init(Queue *q, int cap) {
    q->data = (int *)malloc(cap * sizeof(int));
    q->head = q->tail = 0;
    q->cap = cap;
}
static void queue_push(Queue *q, int v) {
    q->data[q->tail++] = v;
}
static int queue_pop(Queue *q) {
    return q->data[q->head++];
}
static int queue_empty(Queue *q) {
    return q->head >= q->tail;
}
static void queue_free(Queue *q) {
    free(q->data);
}

// Phase 1: Build trie (goto function) from patterns. Returns state count.
static int ac_build_trie(CBMAutomaton *ac, const char **patterns, const int *lengths, int count) {
    int alpha_size = ac->alpha_size;
    int num_states = CBM_AC_ROOT_STATES; // state 0 = root

    for (int p = 0; p < count; p++) {
        int state = 0;
        for (int j = 0; j < lengths[p]; j++) {
            int c = ac->alpha_map[(unsigned char)patterns[p][j]];
            int idx = (state * alpha_size) + c;
            if (ac->go_table[idx] == CBM_AC_NO_STATE) {
                ac->go_table[idx] = num_states++;
            }
            state = ac->go_table[idx];
        }
        if (p < CBM_AC_MAX_BITMASK) {
            ac->output[state] |= CBM_AC_PATTERN_BIT(p);
        }
        ac->output_list[state] = p;
    }

    // Root self-loops for unmatched bytes.
    for (int c = 0; c < alpha_size; c++) {
        if (ac->go_table[c] == CBM_AC_NO_STATE) {
            ac->go_table[c] = 0;
        }
    }
    return num_states;
}

// Phase 2: Build failure function via BFS + compute full goto table.
static void ac_build_failure(CBMAutomaton *ac, int num_states) {
    int alpha_size = ac->alpha_size;
    int *fail = (int *)calloc(num_states, sizeof(int));

    Queue q;
    queue_init(&q, num_states);

    for (int c = 0; c < alpha_size; c++) {
        int s = ac->go_table[c];
        if (s != 0) {
            fail[s] = 0;
            queue_push(&q, s);
        }
    }

    while (!queue_empty(&q)) {
        int r = queue_pop(&q);
        for (int c = 0; c < alpha_size; c++) {
            int idx = (r * alpha_size) + c;
            int s = ac->go_table[idx];
            if (s != CBM_AC_NO_STATE) {
                fail[s] = ac->go_table[(fail[r] * alpha_size) + c];
                ac->output[s] |= ac->output[fail[s]];
                if (ac->output_next[s] == CBM_AC_NO_STATE &&
                    ac->output_list[fail[s]] != CBM_AC_NO_STATE) {
                    ac->output_next[s] = fail[s];
                }
                queue_push(&q, s);
            } else {
                ac->go_table[idx] = ac->go_table[(fail[r] * alpha_size) + c];
            }
        }
    }

    free(fail);
    queue_free(&q);
}

// Shrink allocations to exact state count.
static void ac_shrink_tables(CBMAutomaton *ac, int num_states, int max_states) {
    if (num_states >= max_states) {
        return;
    }
    int alpha_size = ac->alpha_size;
    void *tmp;
    tmp = realloc(ac->go_table, (size_t)num_states * alpha_size * sizeof(int));
    if (tmp) {
        ac->go_table = (int *)tmp;
    }
    tmp = realloc(ac->output, (size_t)num_states * sizeof(uint64_t));
    if (tmp) {
        ac->output = (uint64_t *)tmp;
    }
    tmp = realloc(ac->output_list, (size_t)num_states * sizeof(int));
    if (tmp) {
        ac->output_list = (int *)tmp;
    }
    tmp = realloc(ac->output_next, (size_t)num_states * sizeof(int));
    if (tmp) {
        ac->output_next = (int *)tmp;
    }
}

// cbm_ac_build constructs an Aho-Corasick automaton from a set of patterns.
//
// Parameters:
//   patterns    — array of pattern pointers (not necessarily NUL-terminated)
//   lengths     — length of each pattern
//   count       — number of patterns (max 64 for bitmask mode)
//   alpha_map   — byte→index mapping (NULL = identity/256). For compact alphabets,
//                 map relevant chars to 1..N and everything else to 0.
//   alpha_size  — alphabet size (256 if alpha_map is NULL)
//
// Returns a heap-allocated automaton. Caller must call cbm_ac_free().
CBMAutomaton *cbm_ac_build(const char **patterns, const int *lengths, int count,
                           const uint8_t *alpha_map, int alpha_size) {
    if (count <= 0) {
        return NULL;
    }
    if (alpha_size <= 0) {
        alpha_size = CBM_AC_BYTE_RANGE;
    }

    int max_states = CBM_AC_ROOT_STATES;
    for (int i = 0; i < count; i++) {
        max_states += lengths[i];
    }

    CBMAutomaton *ac = (CBMAutomaton *)calloc(CBM_AC_ALLOC_ONE, sizeof(CBMAutomaton));
    ac->alpha_size = alpha_size;
    ac->num_patterns = count;

    if (alpha_map) {
        memcpy(ac->alpha_map, alpha_map, CBM_AC_BYTE_RANGE);
    } else {
        for (int i = 0; i < CBM_AC_BYTE_RANGE; i++) {
            ac->alpha_map[i] = (uint8_t)i;
        }
    }

    ac->go_table = (int *)malloc((size_t)max_states * alpha_size * sizeof(int));
    memset(ac->go_table, CBM_AC_NO_STATE, (size_t)max_states * alpha_size * sizeof(int));
    ac->output = (uint64_t *)calloc(max_states, sizeof(uint64_t));
    ac->output_list = (int *)malloc(max_states * sizeof(int));
    ac->output_next = (int *)malloc(max_states * sizeof(int));
    for (int i = 0; i < max_states; i++) {
        ac->output_list[i] = CBM_AC_NO_STATE;
        ac->output_next[i] = CBM_AC_NO_STATE;
    }

    int num_states = ac_build_trie(ac, patterns, lengths, count);
    ac_build_failure(ac, num_states);
    ac->num_states = num_states;
    ac_shrink_tables(ac, num_states, max_states);

    return ac;
}

// cbm_ac_free releases all memory for an automaton.
void cbm_ac_free(CBMAutomaton *ac) {
    if (!ac) {
        return;
    }
    free(ac->go_table);
    free(ac->output);
    free(ac->output_list);
    free(ac->output_next);
    free(ac);
}

// ─── Scan functions ────────────────────────────────────────────────────────

// cbm_ac_scan_bitmask scans text through the automaton and returns a bitmask
// of all matched pattern IDs (patterns 0..63).
uint64_t cbm_ac_scan_bitmask(const CBMAutomaton *ac, const char *text, int text_len) {
    uint64_t result = 0;
    int state = 0;
    const int alpha_size = ac->alpha_size;
    const int *go_table = ac->go_table;
    const uint64_t *output = ac->output;

    for (int i = 0; i < text_len; i++) {
        int c = ac->alpha_map[(unsigned char)text[i]];
        state = go_table[(state * alpha_size) + c];
        result |= output[state];
    }
    return result;
}

// ─── Fused LZ4 + AC scan ──────────────────────────────────────────────────

// Thread-local reusable decompression buffer to avoid repeated malloc/free.
// Each goroutine gets its own OS thread (via CGo), so CBM_TLS is safe.
static CBM_TLS char *tls_decomp_buf = NULL;
static CBM_TLS int tls_decomp_cap = 0;

static char *get_decomp_buf(int needed) {
    if (needed > tls_decomp_cap) {
        free(tls_decomp_buf);
        // Round up to 64KB chunks for reuse.
        int cap = (needed + DECOMP_BUF_ALIGN_MASK) & ~DECOMP_BUF_ALIGN_MASK;
        tls_decomp_buf = (cap > 0) ? (char *)malloc((size_t)cap) : NULL;
        tls_decomp_cap = cap;
    }
    return tls_decomp_buf;
}

// cbm_ac_scan_lz4_bitmask decompresses LZ4 data into a thread-local buffer
// and scans it through the AC automaton. Returns bitmask of matched patterns.
// Zero Go heap allocation — the decompression buffer lives in C.
uint64_t cbm_ac_scan_lz4_bitmask(const CBMAutomaton *ac, const char *compressed, int compressed_len,
                                 int original_len) {
    if (!ac || !compressed || compressed_len <= 0 || original_len <= 0) {
        return 0;
    }

    char *buf = get_decomp_buf(original_len);
    if (!buf) {
        return 0;
    }

    int decompressed = cbm_lz4_decompress(compressed, compressed_len, buf, original_len);
    if (decompressed < 0) {
        return 0;
    }

    return cbm_ac_scan_bitmask(ac, buf, decompressed);
}

// ─── Batch LZ4 + AC scan ───────────────────────────────────────────────────

// CBMLz4Entry and CBMLz4Match defined in ac.h.

// cbm_ac_scan_lz4_batch decompresses and scans multiple files in one call.
// Returns the number of matching files written to out_matches.
// Uses a single reusable decompression buffer across all files.
int cbm_ac_scan_lz4_batch(const CBMAutomaton *ac, const CBMLz4Entry *entries, int num_entries,
                          CBMLz4Match *out_matches, int max_matches) {
    if (!ac || !entries || num_entries <= 0) {
        return 0;
    }

    // Allocate decompression buffer sized to the largest file.
    int max_orig = 0;
    for (int i = 0; i < num_entries; i++) {
        if (entries[i].original_len > max_orig) {
            max_orig = entries[i].original_len;
        }
    }
    char *buf = get_decomp_buf(max_orig);
    if (!buf) {
        return 0;
    }

    const int alpha_size = ac->alpha_size;
    const int *go_table = ac->go_table;
    const uint64_t *output = ac->output;
    int total = 0;

    for (int i = 0; i < num_entries && total < max_matches; i++) {
        if (!entries[i].data || entries[i].compressed_len <= 0 || entries[i].original_len <= 0) {
            continue;
        }

        int decompressed = cbm_lz4_decompress(entries[i].data, entries[i].compressed_len, buf,
                                              entries[i].original_len);
        if (decompressed <= 0) {
            continue;
        }

        // Inline AC scan for speed (avoid function call overhead per file).
        uint64_t result = 0;
        int state = 0;
        for (int j = 0; j < decompressed; j++) {
            int c = ac->alpha_map[(unsigned char)buf[j]];
            state = go_table[(state * alpha_size) + c];
            result |= output[state];
        }

        if (result != 0) {
            out_matches[total].file_index = i;
            out_matches[total].bitmask = result;
            total++;
        }
    }

    return total;
}

// ─── Batch scan for configlinker ───────────────────────────────────────────

// CBMMatchResult defined in ac.h.

// cbm_ac_scan_batch scans multiple NUL-separated names through the automaton.
// For each name, reports all unique matched pattern IDs.
// Returns total number of matches written to out_matches.
//
// Parameters:
//   ac           — automaton
//   names_buf    — concatenated names separated by NUL bytes
//   name_offsets — start offset of each name in names_buf
//   name_lengths — length of each name
//   num_names    — number of names
//   out_matches  — output buffer for (name_index, pattern_id) pairs
//   max_matches  — capacity of out_matches
int cbm_ac_scan_batch(const CBMAutomaton *ac, const char *names_buf, const int *name_offsets,
                      const int *name_lengths, int num_names, CBMMatchResult *out_matches,
                      int max_matches) {
    int total = 0;
    const int alpha_size = ac->alpha_size;
    const int *go_table = ac->go_table;

    for (int n = 0; n < num_names && total < max_matches; n++) {
        const char *text = names_buf + name_offsets[n];
        int text_len = name_lengths[n];
        int state = 0;

        // Track which patterns matched for this name (deduplicate).
        uint64_t seen = 0;

        for (int i = 0; i < text_len; i++) {
            int c = ac->alpha_map[(unsigned char)text[i]];
            state = go_table[(state * alpha_size) + c];

            // Walk output chain for >64 patterns.
            int s = state;
            while (s > 0 && total < max_matches) {
                // Bitmask fast path for first 64 patterns.
                uint64_t bits = ac->output[s] & ~seen;
                while (bits && total < max_matches) {
                    int pid = __builtin_ctzll(bits);
                    out_matches[total].name_index = n;
                    out_matches[total].pattern_id = pid;
                    total++;
                    seen |= CBM_AC_PATTERN_BIT(pid);
                    bits = CBM_AC_CLEAR_LOW_BIT(bits);
                }

                // Follow output_next for patterns beyond bitmask range.
                int next_state = ac->output_next[s];
                if (next_state == CBM_AC_NO_STATE || next_state == s) {
                    break;
                }
                s = next_state;
            }
        }
    }
    return total;
}

// ─── Info ──────────────────────────────────────────────────────────────────

int cbm_ac_num_states(const CBMAutomaton *ac) {
    return ac ? ac->num_states : 0;
}

int cbm_ac_num_patterns(const CBMAutomaton *ac) {
    return ac ? ac->num_patterns : 0;
}

// cbm_ac_table_bytes returns the approximate memory used by the goto table.
int cbm_ac_table_bytes(const CBMAutomaton *ac) {
    if (!ac) {
        return 0;
    }
    return ac->num_states * ac->alpha_size * (int)sizeof(int);
}
