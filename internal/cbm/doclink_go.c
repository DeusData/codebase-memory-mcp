/*
 * doclink_go.c — Go doc links and the Go doc-link scope.
 *
 * Tokens. A Go doc comment has no link markup of its own kind: a doc link is
 * bracketed text that go/doc/comment decides to treat as one. This file ports
 * the half of that decision that needs only the comment (go/ast
 * CommentGroup.Text, then go/doc/comment unindent, parseSpans, link
 * definitions, parseLinkedText's bracket scan with its border rule, and the
 * name shape of docLink/splitDocName). What it emits are CANDIDATES:
 *   [Name]  [Recv.Name]  [pkg]  [pkg.Name]  [pkg.Recv.Name]
 * each optionally with a leading `*`, where pkg is an identifier or an import
 * path. The other half of the decision (does the package declare the name,
 * does the file import the package) needs the whole package and is the
 * resolver's (src/pipeline/doc_links_go.c): a candidate that fails it is plain
 * text, exactly as go/doc renders it, and leaves neither an edge nor a row.
 * A `[Text]` with a link definition `[Text]: scheme://...` in the same comment
 * is a URL: one `href` token per use.
 *
 * Never a link: code blocks (indented lines), `# ` headings (go/doc keeps
 * heading text plain), the link-definition paragraphs themselves, and a
 * bracket pair that touches a letter or digit (`map[K]V`, `a[i]`).
 *
 * Scope blob (first line "go1", then one record per line, tab-separated,
 * sorted; only exported names are recorded, a doc link can name nothing else):
 *   P  name  flags          package clause; flags: i build-ignored file
 *                           (`//go:build ignore`), c another build-constraint
 *                           line, e parse errors in the file, - none
 *   I  local  path          import; local `-` when not renamed (dot and blank
 *                           imports are no qualifiers and are left out); path
 *                           `?` when it is none this scan can record
 *   F  name  w              function
 *   M  recv  name  w        method (recv `?` when the receiver type is not a
 *                           plain name)
 *   T  name  kind  alias  w type; kind s struct, i interface, a alias, o other;
 *                           alias: the target of `type X = Y` / `= pkg.Y`
 *   V  name  w              variable or constant
 *   f  type  name  node     struct field (node 0: embedded, or not the first
 *                           name of its declaration)
 *   m  type  name           interface method
 *   g  first  last  names   type parameters (','-joined) of the declaration
 *                           that spans the lines first..last. Only in the
 *                           blob of a fresh extraction; the persisted blob
 *                           drops these lines and so carries no line number
 *                           at all.
 *   G  path                 go.mod: the module path
 * w says who keeps the node. A Go method node has a FLAT qualified name
 * (<dir>.<Method>), so a function, the methods of every receiver, a type and
 * a value of one name all claim the node <dir>.<name>, and within a file the
 * declaration on the LAST line keeps it (cbm_gbuf_upsert_node). w is 1 for
 * that declaration, 0 for the others of that name, x when two of them start
 * on that last line, and - for a value the extractor gives no node at all
 * (it gives one to the first name of a constant spec and of a parenthesis-
 * free `var`).
 * The records are sorted and w is the only order-dependent field: the blob
 * does not change when declarations merely move, and it does change when a
 * move hands a node to another declaration.
 */
#include "doclink.h"

#include "cbm.h"
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "tree_sitter/api.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    GO_REF_MAX = CBM_DOCLINK_GO_REF_MAX, /* longest bracket text and name handled */
    GO_URL_MAX = 2048,                   /* a link-definition URL is stored up to here */
    GO_PATH_MAX = 1024,                  /* longest import or module path recorded */
    GO_ARRAY_INIT = 16,
    GO_MARKER_LEN = 2,   /* "//", the opening and the closing of a block comment */
    GO_LINKDEF_TAIL = 3, /* "]: " */
    GO_ASCII_END = 0x80, /* first non-ASCII byte */
    GO_ASCII_DEL = 0x7F,
    GO_RECV_UNWRAP = 4,  /* pointer_type / generic_type levels of a receiver, as the extractor */
    GO_EMBED_UNWRAP = 8, /* the same for an embedded field: *pkg.T[A] and some slack */
    GO_SB_INIT = 64,
    GO_NODE_FLAG = 1, /* "node" field of a V or f record */
};

/* ── Small helpers ───────────────────────────────────────────────── */

static CBMArena *go_scratch(CBMExtractCtx *ctx) {
    return ctx->scratch ? ctx->scratch : ctx->arena;
}

/* Room for one more element of an arena array (the old block is abandoned to
 * the arena); NULL when memory ran out. */
static void *go_grow(CBMArena *a, void *items, int count, int *cap, size_t elem) {
    if (count < *cap) {
        return items;
    }
    int ncap = *cap ? *cap * PAIR_LEN : GO_ARRAY_INIT;
    void *grown = cbm_arena_alloc(a, (size_t)ncap * elem);
    if (!grown) {
        return NULL;
    }
    if (count > 0) {
        memcpy(grown, items, (size_t)count * elem);
    }
    *cap = ncap;
    return grown;
}

static bool go_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

static bool go_blank_or_tab(char c) {
    return c == ' ' || c == '\t';
}

static bool go_has_prefix(const char *s, size_t n, const char *prefix) {
    size_t pl = strlen(prefix);
    return n >= pl && memcmp(s, prefix, pl) == 0;
}

static bool go_ascii_alpha(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static bool go_ascii_digit(unsigned char c) {
    return c >= '0' && c <= '9';
}

static void go_trim(const char **s, size_t *n) {
    while (*n > 0 && go_space((*s)[0])) {
        (*s)++;
        (*n)--;
    }
    while (*n > 0 && go_space((*s)[*n - SKIP_ONE])) {
        (*n)--;
    }
}

/* ── UTF-8 and the two Unicode classes go/doc/comment asks for ───── */

typedef struct {
    uint32_t lo;
    uint32_t hi;
    uint32_t stride;
} go_range_t;

/* General category P* above U+007F (Unicode 16.0.0), ascending. */
static const go_range_t GO_PUNCT[] = {
    {0x00A1, 0x00A1, 1},   {0x00A7, 0x00A7, 1},   {0x00AB, 0x00AB, 1},   {0x00B6, 0x00B7, 1},
    {0x00BB, 0x00BB, 1},   {0x00BF, 0x00BF, 1},   {0x037E, 0x037E, 1},   {0x0387, 0x0387, 1},
    {0x055A, 0x055F, 1},   {0x0589, 0x058A, 1},   {0x05BE, 0x05BE, 1},   {0x05C0, 0x05C0, 1},
    {0x05C3, 0x05C3, 1},   {0x05C6, 0x05C6, 1},   {0x05F3, 0x05F4, 1},   {0x0609, 0x060A, 1},
    {0x060C, 0x060D, 1},   {0x061B, 0x061B, 1},   {0x061D, 0x061F, 1},   {0x066A, 0x066D, 1},
    {0x06D4, 0x06D4, 1},   {0x0700, 0x070D, 1},   {0x07F7, 0x07F9, 1},   {0x0830, 0x083E, 1},
    {0x085E, 0x085E, 1},   {0x0964, 0x0965, 1},   {0x0970, 0x0970, 1},   {0x09FD, 0x09FD, 1},
    {0x0A76, 0x0A76, 1},   {0x0AF0, 0x0AF0, 1},   {0x0C77, 0x0C77, 1},   {0x0C84, 0x0C84, 1},
    {0x0DF4, 0x0DF4, 1},   {0x0E4F, 0x0E4F, 1},   {0x0E5A, 0x0E5B, 1},   {0x0F04, 0x0F12, 1},
    {0x0F14, 0x0F14, 1},   {0x0F3A, 0x0F3D, 1},   {0x0F85, 0x0F85, 1},   {0x0FD0, 0x0FD4, 1},
    {0x0FD9, 0x0FDA, 1},   {0x104A, 0x104F, 1},   {0x10FB, 0x10FB, 1},   {0x1360, 0x1368, 1},
    {0x1400, 0x1400, 1},   {0x166E, 0x166E, 1},   {0x169B, 0x169C, 1},   {0x16EB, 0x16ED, 1},
    {0x1735, 0x1736, 1},   {0x17D4, 0x17D6, 1},   {0x17D8, 0x17DA, 1},   {0x1800, 0x180A, 1},
    {0x1944, 0x1945, 1},   {0x1A1E, 0x1A1F, 1},   {0x1AA0, 0x1AA6, 1},   {0x1AA8, 0x1AAD, 1},
    {0x1B4E, 0x1B4F, 1},   {0x1B5A, 0x1B60, 1},   {0x1B7D, 0x1B7F, 1},   {0x1BFC, 0x1BFF, 1},
    {0x1C3B, 0x1C3F, 1},   {0x1C7E, 0x1C7F, 1},   {0x1CC0, 0x1CC7, 1},   {0x1CD3, 0x1CD3, 1},
    {0x2010, 0x2027, 1},   {0x2030, 0x2043, 1},   {0x2045, 0x2051, 1},   {0x2053, 0x205E, 1},
    {0x207D, 0x207E, 1},   {0x208D, 0x208E, 1},   {0x2308, 0x230B, 1},   {0x2329, 0x232A, 1},
    {0x2768, 0x2775, 1},   {0x27C5, 0x27C6, 1},   {0x27E6, 0x27EF, 1},   {0x2983, 0x2998, 1},
    {0x29D8, 0x29DB, 1},   {0x29FC, 0x29FD, 1},   {0x2CF9, 0x2CFC, 1},   {0x2CFE, 0x2CFF, 1},
    {0x2D70, 0x2D70, 1},   {0x2E00, 0x2E2E, 1},   {0x2E30, 0x2E4F, 1},   {0x2E52, 0x2E5D, 1},
    {0x3001, 0x3003, 1},   {0x3008, 0x3011, 1},   {0x3014, 0x301F, 1},   {0x3030, 0x3030, 1},
    {0x303D, 0x303D, 1},   {0x30A0, 0x30A0, 1},   {0x30FB, 0x30FB, 1},   {0xA4FE, 0xA4FF, 1},
    {0xA60D, 0xA60F, 1},   {0xA673, 0xA673, 1},   {0xA67E, 0xA67E, 1},   {0xA6F2, 0xA6F7, 1},
    {0xA874, 0xA877, 1},   {0xA8CE, 0xA8CF, 1},   {0xA8F8, 0xA8FA, 1},   {0xA8FC, 0xA8FC, 1},
    {0xA92E, 0xA92F, 1},   {0xA95F, 0xA95F, 1},   {0xA9C1, 0xA9CD, 1},   {0xA9DE, 0xA9DF, 1},
    {0xAA5C, 0xAA5F, 1},   {0xAADE, 0xAADF, 1},   {0xAAF0, 0xAAF1, 1},   {0xABEB, 0xABEB, 1},
    {0xFD3E, 0xFD3F, 1},   {0xFE10, 0xFE19, 1},   {0xFE30, 0xFE52, 1},   {0xFE54, 0xFE61, 1},
    {0xFE63, 0xFE63, 1},   {0xFE68, 0xFE68, 1},   {0xFE6A, 0xFE6B, 1},   {0xFF01, 0xFF03, 1},
    {0xFF05, 0xFF0A, 1},   {0xFF0C, 0xFF0F, 1},   {0xFF1A, 0xFF1B, 1},   {0xFF1F, 0xFF20, 1},
    {0xFF3B, 0xFF3D, 1},   {0xFF3F, 0xFF3F, 1},   {0xFF5B, 0xFF5B, 1},   {0xFF5D, 0xFF5D, 1},
    {0xFF5F, 0xFF65, 1},   {0x10100, 0x10102, 1}, {0x1039F, 0x1039F, 1}, {0x103D0, 0x103D0, 1},
    {0x1056F, 0x1056F, 1}, {0x10857, 0x10857, 1}, {0x1091F, 0x1091F, 1}, {0x1093F, 0x1093F, 1},
    {0x10A50, 0x10A58, 1}, {0x10A7F, 0x10A7F, 1}, {0x10AF0, 0x10AF6, 1}, {0x10B39, 0x10B3F, 1},
    {0x10B99, 0x10B9C, 1}, {0x10D6E, 0x10D6E, 1}, {0x10EAD, 0x10EAD, 1}, {0x10F55, 0x10F59, 1},
    {0x10F86, 0x10F89, 1}, {0x11047, 0x1104D, 1}, {0x110BB, 0x110BC, 1}, {0x110BE, 0x110C1, 1},
    {0x11140, 0x11143, 1}, {0x11174, 0x11175, 1}, {0x111C5, 0x111C8, 1}, {0x111CD, 0x111CD, 1},
    {0x111DB, 0x111DB, 1}, {0x111DD, 0x111DF, 1}, {0x11238, 0x1123D, 1}, {0x112A9, 0x112A9, 1},
    {0x113D4, 0x113D5, 1}, {0x113D7, 0x113D8, 1}, {0x1144B, 0x1144F, 1}, {0x1145A, 0x1145B, 1},
    {0x1145D, 0x1145D, 1}, {0x114C6, 0x114C6, 1}, {0x115C1, 0x115D7, 1}, {0x11641, 0x11643, 1},
    {0x11660, 0x1166C, 1}, {0x116B9, 0x116B9, 1}, {0x1173C, 0x1173E, 1}, {0x1183B, 0x1183B, 1},
    {0x11944, 0x11946, 1}, {0x119E2, 0x119E2, 1}, {0x11A3F, 0x11A46, 1}, {0x11A9A, 0x11A9C, 1},
    {0x11A9E, 0x11AA2, 1}, {0x11B00, 0x11B09, 1}, {0x11BE1, 0x11BE1, 1}, {0x11C41, 0x11C45, 1},
    {0x11C70, 0x11C71, 1}, {0x11EF7, 0x11EF8, 1}, {0x11F43, 0x11F4F, 1}, {0x11FFF, 0x11FFF, 1},
    {0x12470, 0x12474, 1}, {0x12FF1, 0x12FF2, 1}, {0x16A6E, 0x16A6F, 1}, {0x16AF5, 0x16AF5, 1},
    {0x16B37, 0x16B3B, 1}, {0x16B44, 0x16B44, 1}, {0x16D6D, 0x16D6F, 1}, {0x16E97, 0x16E9A, 1},
    {0x16FE2, 0x16FE2, 1}, {0x1BC9F, 0x1BC9F, 1}, {0x1DA87, 0x1DA8B, 1}, {0x1E5FF, 0x1E5FF, 1},
    {0x1E95E, 0x1E95F, 1},
};

/* General category Lu above U+007F (Unicode 16.0.0), ascending; a stride of 2
 * covers the runs where upper and lower case alternate. */
static const go_range_t GO_UPPER[] = {
    {0x00C0, 0x00D6, 1},   {0x00D8, 0x00DE, 1},   {0x0100, 0x0136, 2},   {0x0139, 0x0147, 2},
    {0x014A, 0x0176, 2},   {0x0178, 0x0179, 1},   {0x017B, 0x017D, 2},   {0x0181, 0x0182, 1},
    {0x0184, 0x0184, 1},   {0x0186, 0x0187, 1},   {0x0189, 0x018B, 1},   {0x018E, 0x0191, 1},
    {0x0193, 0x0194, 1},   {0x0196, 0x0198, 1},   {0x019C, 0x019D, 1},   {0x019F, 0x01A0, 1},
    {0x01A2, 0x01A4, 2},   {0x01A6, 0x01A7, 1},   {0x01A9, 0x01A9, 1},   {0x01AC, 0x01AC, 1},
    {0x01AE, 0x01AF, 1},   {0x01B1, 0x01B3, 1},   {0x01B5, 0x01B5, 1},   {0x01B7, 0x01B8, 1},
    {0x01BC, 0x01BC, 1},   {0x01C4, 0x01C4, 1},   {0x01C7, 0x01C7, 1},   {0x01CA, 0x01CA, 1},
    {0x01CD, 0x01DB, 2},   {0x01DE, 0x01EE, 2},   {0x01F1, 0x01F1, 1},   {0x01F4, 0x01F4, 1},
    {0x01F6, 0x01F8, 1},   {0x01FA, 0x0232, 2},   {0x023A, 0x023B, 1},   {0x023D, 0x023E, 1},
    {0x0241, 0x0241, 1},   {0x0243, 0x0246, 1},   {0x0248, 0x024E, 2},   {0x0370, 0x0372, 2},
    {0x0376, 0x0376, 1},   {0x037F, 0x037F, 1},   {0x0386, 0x0386, 1},   {0x0388, 0x038A, 1},
    {0x038C, 0x038C, 1},   {0x038E, 0x038F, 1},   {0x0391, 0x03A1, 1},   {0x03A3, 0x03AB, 1},
    {0x03CF, 0x03CF, 1},   {0x03D2, 0x03D4, 1},   {0x03D8, 0x03EE, 2},   {0x03F4, 0x03F4, 1},
    {0x03F7, 0x03F7, 1},   {0x03F9, 0x03FA, 1},   {0x03FD, 0x042F, 1},   {0x0460, 0x0480, 2},
    {0x048A, 0x04BE, 2},   {0x04C0, 0x04C1, 1},   {0x04C3, 0x04CD, 2},   {0x04D0, 0x052E, 2},
    {0x0531, 0x0556, 1},   {0x10A0, 0x10C5, 1},   {0x10C7, 0x10C7, 1},   {0x10CD, 0x10CD, 1},
    {0x13A0, 0x13F5, 1},   {0x1C89, 0x1C89, 1},   {0x1C90, 0x1CBA, 1},   {0x1CBD, 0x1CBF, 1},
    {0x1E00, 0x1E94, 2},   {0x1E9E, 0x1EFE, 2},   {0x1F08, 0x1F0F, 1},   {0x1F18, 0x1F1D, 1},
    {0x1F28, 0x1F2F, 1},   {0x1F38, 0x1F3F, 1},   {0x1F48, 0x1F4D, 1},   {0x1F59, 0x1F5F, 2},
    {0x1F68, 0x1F6F, 1},   {0x1FB8, 0x1FBB, 1},   {0x1FC8, 0x1FCB, 1},   {0x1FD8, 0x1FDB, 1},
    {0x1FE8, 0x1FEC, 1},   {0x1FF8, 0x1FFB, 1},   {0x2102, 0x2102, 1},   {0x2107, 0x2107, 1},
    {0x210B, 0x210D, 1},   {0x2110, 0x2112, 1},   {0x2115, 0x2115, 1},   {0x2119, 0x211D, 1},
    {0x2124, 0x2128, 2},   {0x212A, 0x212D, 1},   {0x2130, 0x2133, 1},   {0x213E, 0x213F, 1},
    {0x2145, 0x2145, 1},   {0x2183, 0x2183, 1},   {0x2C00, 0x2C2F, 1},   {0x2C60, 0x2C60, 1},
    {0x2C62, 0x2C64, 1},   {0x2C67, 0x2C6B, 2},   {0x2C6D, 0x2C70, 1},   {0x2C72, 0x2C72, 1},
    {0x2C75, 0x2C75, 1},   {0x2C7E, 0x2C80, 1},   {0x2C82, 0x2CE2, 2},   {0x2CEB, 0x2CED, 2},
    {0x2CF2, 0x2CF2, 1},   {0xA640, 0xA66C, 2},   {0xA680, 0xA69A, 2},   {0xA722, 0xA72E, 2},
    {0xA732, 0xA76E, 2},   {0xA779, 0xA77B, 2},   {0xA77D, 0xA77E, 1},   {0xA780, 0xA786, 2},
    {0xA78B, 0xA78D, 2},   {0xA790, 0xA792, 2},   {0xA796, 0xA7A8, 2},   {0xA7AA, 0xA7AE, 1},
    {0xA7B0, 0xA7B4, 1},   {0xA7B6, 0xA7C2, 2},   {0xA7C4, 0xA7C7, 1},   {0xA7C9, 0xA7C9, 1},
    {0xA7CB, 0xA7CC, 1},   {0xA7D0, 0xA7D0, 1},   {0xA7D6, 0xA7DC, 2},   {0xA7F5, 0xA7F5, 1},
    {0xFF21, 0xFF3A, 1},   {0x10400, 0x10427, 1}, {0x104B0, 0x104D3, 1}, {0x10570, 0x1057A, 1},
    {0x1057C, 0x1058A, 1}, {0x1058C, 0x10592, 1}, {0x10594, 0x10595, 1}, {0x10C80, 0x10CB2, 1},
    {0x10D50, 0x10D65, 1}, {0x118A0, 0x118BF, 1}, {0x16E40, 0x16E5F, 1}, {0x1D400, 0x1D419, 1},
    {0x1D434, 0x1D44D, 1}, {0x1D468, 0x1D481, 1}, {0x1D49C, 0x1D49C, 1}, {0x1D49E, 0x1D49F, 1},
    {0x1D4A2, 0x1D4A2, 1}, {0x1D4A5, 0x1D4A6, 1}, {0x1D4A9, 0x1D4AC, 1}, {0x1D4AE, 0x1D4B5, 1},
    {0x1D4D0, 0x1D4E9, 1}, {0x1D504, 0x1D505, 1}, {0x1D507, 0x1D50A, 1}, {0x1D50D, 0x1D514, 1},
    {0x1D516, 0x1D51C, 1}, {0x1D538, 0x1D539, 1}, {0x1D53B, 0x1D53E, 1}, {0x1D540, 0x1D544, 1},
    {0x1D546, 0x1D546, 1}, {0x1D54A, 0x1D550, 1}, {0x1D56C, 0x1D585, 1}, {0x1D5A0, 0x1D5B9, 1},
    {0x1D5D4, 0x1D5ED, 1}, {0x1D608, 0x1D621, 1}, {0x1D63C, 0x1D655, 1}, {0x1D670, 0x1D689, 1},
    {0x1D6A8, 0x1D6C0, 1}, {0x1D6E2, 0x1D6FA, 1}, {0x1D71C, 0x1D734, 1}, {0x1D756, 0x1D76E, 1},
    {0x1D790, 0x1D7A8, 1}, {0x1D7CA, 0x1D7CA, 1}, {0x1E900, 0x1E921, 1},
};

enum {
    GO_PUNCT_COUNT = sizeof(GO_PUNCT) / sizeof(GO_PUNCT[0]),
    GO_UPPER_COUNT = sizeof(GO_UPPER) / sizeof(GO_UPPER[0]),
    GO_U8_CONT_MASK = 0xC0,
    GO_U8_CONT = 0x80,
    GO_U8_PAYLOAD = 0x3F,
    GO_U8_SHIFT = 6,
    GO_U8_LEAD2 = 0xC0,
    GO_U8_LEAD3 = 0xE0,
    GO_U8_LEAD4 = 0xF0,
    GO_U8_LEAD_END = 0xF8,
    GO_U8_MASK2 = 0x1F,
    GO_U8_MASK3 = 0x0F,
    GO_U8_MASK4 = 0x07,
    GO_U8_MIN2 = 0x80,
    GO_U8_MIN3 = 0x800,
    GO_U8_MIN4 = 0x10000,
    GO_U8_LEN3 = 3,
    GO_U8_LEN4 = 4,
    GO_SURROGATE_LO = 0xD800,
    GO_SURROGATE_HI = 0xDFFF,
    GO_RUNE_MAX = 0x10FFFF,
};

/* Not a rune: invalid UTF-8. */
static const uint32_t GO_RUNE_BAD = 0xFFFFFFFFU;

/* Length of the sequence a lead byte announces; 0 for a byte that leads none. */
static int go_u8_len(unsigned char c) {
    if (c < GO_ASCII_END) {
        return SKIP_ONE;
    }
    if (c < GO_U8_LEAD2 || c >= GO_U8_LEAD_END) {
        return 0;
    }
    if (c < GO_U8_LEAD3) {
        return PAIR_LEN;
    }
    return c < GO_U8_LEAD4 ? GO_U8_LEN3 : GO_U8_LEN4;
}

/* The rune encoded by exactly s[0..n), or GO_RUNE_BAD. Overlong forms,
 * surrogates and values past U+10FFFF are invalid, as for utf8.DecodeRune. */
static uint32_t go_rune_decode(const unsigned char *s, int n) {
    if (n == SKIP_ONE) {
        return s[0];
    }
    static const uint32_t min_value[] = {0, 0, GO_U8_MIN2, GO_U8_MIN3, GO_U8_MIN4};
    static const unsigned char lead_mask[] = {0, 0, GO_U8_MASK2, GO_U8_MASK3, GO_U8_MASK4};
    uint32_t r = (uint32_t)(s[0] & lead_mask[n]);
    for (int i = SKIP_ONE; i < n; i++) {
        if ((s[i] & GO_U8_CONT_MASK) != GO_U8_CONT) {
            return GO_RUNE_BAD;
        }
        r = (r << GO_U8_SHIFT) | (uint32_t)(s[i] & GO_U8_PAYLOAD);
    }
    if (r < min_value[n] || r > GO_RUNE_MAX || (r >= GO_SURROGATE_LO && r <= GO_SURROGATE_HI)) {
        return GO_RUNE_BAD;
    }
    return r;
}

/* The rune that starts at s[at] (s has n bytes, at < n). */
static uint32_t go_rune_at(const char *s, size_t n, size_t at) {
    const unsigned char *u = (const unsigned char *)s + at;
    int len = go_u8_len(u[0]);
    if (len == 0 || (size_t)len > n - at) {
        return GO_RUNE_BAD;
    }
    return go_rune_decode(u, len);
}

/* The rune that ends just before s[end] (end > 0). */
static uint32_t go_rune_before(const char *s, size_t end) {
    const unsigned char *u = (const unsigned char *)s;
    size_t start = end - SKIP_ONE;
    int back = SKIP_ONE;
    while (start > 0 && back < GO_U8_LEN4 && (u[start] & GO_U8_CONT_MASK) == GO_U8_CONT) {
        start--;
        back++;
    }
    if (go_u8_len(u[start]) != back) {
        return GO_RUNE_BAD;
    }
    return go_rune_decode(u + start, back);
}

static bool go_in_ranges(const go_range_t *ranges, int count, uint32_t r) {
    int lo = 0;
    int hi = count - SKIP_ONE;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (r < ranges[mid].lo) {
            hi = mid - SKIP_ONE;
        } else if (r > ranges[mid].hi) {
            lo = mid + SKIP_ONE;
        } else {
            return (r - ranges[mid].lo) % ranges[mid].stride == 0;
        }
    }
    return false;
}

/* unicode.IsPunct. */
static bool go_is_punct(uint32_t r) {
    if (r < GO_ASCII_END) {
        return r != 0 && strchr("!\"#%&'()*,-./:;?@[\\]_{}", (int)r) != NULL;
    }
    return go_in_ranges(GO_PUNCT, GO_PUNCT_COUNT, r);
}

/* unicode.IsUpper. */
static bool go_is_upper(uint32_t r) {
    if (r < GO_ASCII_END) {
        return r >= 'A' && r <= 'Z';
    }
    return r != GO_RUNE_BAD && go_in_ranges(GO_UPPER, GO_UPPER_COUNT, r);
}

/* docLink's border rule for one neighbour of the brackets. */
static bool go_border_ok(uint32_t r) {
    return r == ' ' || r == '\t' || r == '\n' || (r != GO_RUNE_BAD && go_is_punct(r));
}

/* ── Identifiers and reference shape (docLink, splitDocName) ─────── */

/* A byte of a Go identifier. Every non-ASCII byte counts as a letter: text
 * that is no identifier then names nothing a package declares, and the
 * resolver treats it as plain text. */
static bool go_ident_byte(unsigned char c) {
    return go_ascii_alpha(c) || go_ascii_digit(c) || c == '_' || c >= GO_ASCII_END;
}

/* go/doc/comment ident(): s is one Go identifier. */
static bool go_is_ident(const char *s, size_t n) {
    if (n == 0 || n > GO_REF_MAX || go_ascii_digit((unsigned char)s[0])) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!go_ident_byte((unsigned char)s[i])) {
            return false;
        }
    }
    return true;
}

/* go/doc/comment isName(): an identifier whose first letter is upper case,
 * which is also what makes a Go identifier exported. */
static bool go_is_name(const char *s, size_t n) {
    return go_is_ident(s, n) && go_is_upper(go_rune_at(s, n, 0));
}

static bool go_import_path_byte(unsigned char c) {
    return go_ascii_alpha(c) || go_ascii_digit(c) || c == '-' || c == '.' || c == '_' || c == '~' ||
           c == '+';
}

static bool go_import_elem_ok(const char *s, size_t n) {
    if (n == 0 || s[0] == '.' || s[n - SKIP_ONE] == '.') {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!go_import_path_byte((unsigned char)s[i])) {
            return false;
        }
    }
    return true;
}

/* go/doc/comment validImportPath. */
static bool go_valid_import_path(const char *s, size_t n) {
    if (n == 0 || n > GO_PATH_MAX || s[0] == '-' || s[n - SKIP_ONE] == '/') {
        return false;
    }
    size_t start = 0;
    for (size_t i = 0; i <= n; i++) {
        if (i == n || s[i] == '/') {
            if (!go_import_elem_ok(s + start, i - start)) {
                return false;
            }
            start = i + SKIP_ONE;
        }
    }
    return true;
}

/* An import name as a file can give one: identifier bytes only. */
static bool go_pkg_word(const char *s, size_t n) {
    if (n == 0 || n > GO_REF_MAX) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!go_ident_byte((unsigned char)s[i])) {
            return false;
        }
    }
    return true;
}

/* splitDocName: `before.Name` with a capitalized Name. False when the text
 * after the last dot is no Name (then nothing is split off). */
static bool go_split_name(const char *s, size_t n, size_t *before_len, const char **name,
                          size_t *name_len) {
    size_t dot = n;
    for (size_t i = n; i > 0; i--) {
        if (s[i - SKIP_ONE] == '.') {
            dot = i - SKIP_ONE;
            break;
        }
    }
    size_t ns = dot < n ? dot + SKIP_ONE : 0;
    if (!go_is_name(s + ns, n - ns)) {
        return false;
    }
    *name = s + ns;
    *name_len = n - ns;
    *before_len = dot < n ? dot : 0;
    return true;
}

bool cbm_doclink_go_parse_ref(const char *raw, size_t len, CBMDocLinkGoRef *out) {
    memset(out, 0, sizeof(*out));
    if (!raw || len == 0 || len > GO_REF_MAX) {
        return false;
    }
    const char *s = raw;
    size_t n = len;
    if (s[0] == '*') {
        s++;
        n--;
    }
    size_t before = 0;
    out->pkg = s;
    out->pkg_len = n;
    if (go_split_name(s, n, &before, &out->name, &out->name_len)) {
        size_t pkg_len = 0;
        out->pkg_len = before;
        if (go_split_name(s, before, &pkg_len, &out->recv, &out->recv_len)) {
            out->pkg_len = pkg_len;
        }
    }
    if (out->pkg_len == 0) {
        return out->name_len > 0; /* [Name], [Recv.Name]: the current package */
    }
    if (memchr(out->pkg, '/', out->pkg_len)) {
        return go_valid_import_path(out->pkg, out->pkg_len);
    }
    return go_pkg_word(out->pkg, out->pkg_len);
}

/* Bracket text longer than this scan handles: true when its bytes are those a
 * doc link is made of, so that it is reported instead of dropped. */
static bool go_long_ref_shaped(const char *s, size_t n) {
    size_t i = (n > 0 && s[0] == '*') ? SKIP_ONE : 0;
    if (i >= n) {
        return false;
    }
    for (; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!go_ident_byte(c) && !go_import_path_byte(c) && c != '/') {
            return false;
        }
    }
    return true;
}

/* The longest prefix of s[0..n) of at most `max` bytes that ends on a rune
 * boundary. */
static size_t go_clip(const char *s, size_t n, size_t max) {
    if (n <= max) {
        return n;
    }
    size_t cut = max;
    while (cut > 0 && ((unsigned char)s[cut] & GO_U8_CONT_MASK) == GO_U8_CONT) {
        cut--;
    }
    return cut;
}

/* ── Doc text model ──────────────────────────────────────────────── */

/* One line of the comment text: a slice of the doc string. */
typedef struct {
    const char *s;
    uint32_t len;
    uint32_t line; /* 1-based source line */
} go_line_t;

enum { GO_SPAN_CODE = 1, GO_SPAN_HEADING, GO_SPAN_LIST, GO_SPAN_PARA };

typedef struct {
    int start;
    int end;
    int kind;
} go_span_t;

/* `[text]: url`, one line of a link-definition paragraph. */
typedef struct {
    const char *text;
    size_t text_len;
    const char *url;
    size_t url_len;
    int order;
} go_linkdef_t;

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *tmp;
    const char *source_qn;
    const char **params; /* the documented definition's parameter names, or NULL */
    uint32_t def_line;
    go_line_t *lines;
    int nlines;
    int lines_cap;
    go_span_t *spans;
    int nspans;
    int spans_cap;
    go_linkdef_t *defs;
    int ndefs;
    int defs_cap;
    go_line_t *para; /* the lines of one list-item paragraph */
    bool failed;
} go_doc_t;

static void go_push_line(go_doc_t *d, const char *s, size_t len, uint32_t line) {
    while (len > 0 && go_space(s[len - SKIP_ONE])) {
        len--; /* ast.CommentGroup.Text strips trailing white space */
    }
    go_line_t *grown =
        (go_line_t *)go_grow(d->tmp, d->lines, d->nlines, &d->lines_cap, sizeof(go_line_t));
    if (!grown) {
        d->failed = true;
        return;
    }
    d->lines = grown;
    d->lines[d->nlines++] = (go_line_t){.s = s, .len = (uint32_t)len, .line = line};
}

static bool go_lower_or_digit(char b) {
    return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9');
}

/* go/ast isDirective: //line, //extern, //export and //[a-z0-9]+:[a-z0-9]. */
static bool go_is_directive(const char *c, size_t n) {
    if (go_has_prefix(c, n, "line ") || go_has_prefix(c, n, "extern ") ||
        go_has_prefix(c, n, "export ")) {
        return true;
    }
    const char *colon = (const char *)memchr(c, ':', n);
    if (!colon || colon == c || (size_t)(colon - c) + SKIP_ONE >= n) {
        return false;
    }
    size_t ci = (size_t)(colon - c);
    for (size_t i = 0; i <= ci + SKIP_ONE; i++) {
        if (i != ci && !go_lower_or_digit(c[i])) {
            return false;
        }
    }
    return true;
}

/* The text of one `//` comment (c = what follows the slashes). */
static void go_line_comment(go_doc_t *d, const char *c, size_t n, uint32_t line) {
    if (n > 0 && c[0] == ' ') {
        go_push_line(d, c + SKIP_ONE, n - SKIP_ONE, line);
    } else if (n == 0 || !go_is_directive(c, n)) {
        go_push_line(d, c, n, line);
    }
}

/* The lines of one block comment body [c, end); returns the line breaks in it. */
static uint32_t go_block_comment(go_doc_t *d, const char *c, const char *end, uint32_t line) {
    uint32_t added = 0;
    for (;;) {
        const char *nl = (const char *)memchr(c, '\n', (size_t)(end - c));
        const char *le = nl ? nl : end;
        go_push_line(d, c, (size_t)(le - c), line + added);
        if (!nl) {
            return added;
        }
        added++;
        c = nl + SKIP_ONE;
    }
}

/* ast.CommentGroup.Text over the stored comment group: one comment per text
 * line, a block comment keeping its own lines. Text line k of the group is
 * source line doc_line + k. Every byte is looked at once: a comment is
 * consumed whole, then the scan continues behind its line. */
static void go_group_lines(go_doc_t *d, const char *doc, uint32_t doc_line) {
    const char *p = doc;
    uint32_t line = doc_line;
    while (*p && !d->failed) {
        const char *rest = p;
        if (p[0] == '/' && p[SKIP_ONE] == '/') {
            const char *eol = strchr(p, '\n');
            const char *end = eol ? eol : p + strlen(p);
            go_line_comment(d, p + GO_MARKER_LEN, (size_t)(end - p) - GO_MARKER_LEN, line);
            rest = end;
        } else if (p[0] == '/' && p[SKIP_ONE] == '*') {
            const char *body = p + GO_MARKER_LEN;
            const char *close = strstr(body, "*/");
            const char *end = close ? close : body + strlen(body);
            line += go_block_comment(d, body, end, line);
            rest = close ? close + GO_MARKER_LEN : end;
        }
        const char *eol = strchr(rest, '\n');
        if (!eol) {
            break;
        }
        p = eol + SKIP_ONE;
        line++;
    }
}

static size_t go_leading_space(const go_line_t *l) {
    size_t i = 0;
    while (i < l->len && go_blank_or_tab(l->s[i])) {
        i++;
    }
    return i;
}

/* go/doc/comment unindent: drop the blank lines at both ends and the common
 * space/tab prefix of the others. */
static void go_unindent(go_doc_t *d) {
    int first = 0;
    int last = d->nlines;
    while (first < last && d->lines[first].len == 0) {
        first++;
    }
    while (last > first && d->lines[last - SKIP_ONE].len == 0) {
        last--;
    }
    d->lines += first;
    d->nlines = last - first;
    d->lines_cap -= first;
    if (d->nlines == 0) {
        return;
    }
    const char *prefix = d->lines[0].s;
    size_t plen = go_leading_space(&d->lines[0]);
    for (int i = SKIP_ONE; i < d->nlines && plen > 0; i++) {
        const go_line_t *l = &d->lines[i];
        if (l->len == 0) {
            continue;
        }
        size_t ls = go_leading_space(l);
        size_t k = 0;
        while (k < plen && k < ls && prefix[k] == l->s[k]) {
            k++;
        }
        plen = k;
    }
    for (int i = 0; i < d->nlines && plen > 0; i++) {
        go_line_t *l = &d->lines[i];
        if (l->len >= plen) {
            l->s += plen;
            l->len -= (uint32_t)plen;
        }
    }
}

static bool go_indented(const go_line_t *l) {
    return l->len > 0 && go_blank_or_tab(l->s[0]);
}

/* go/doc/comment listMarker: the text after a bullet (`*`, `+`, `-`, U+2022)
 * or a number followed by `.` or `)`; NULL when the line starts no list item.
 * The marker must be followed by a space or tab and then text. */
static const char *go_list_marker(const go_line_t *l, size_t *rest_len, bool *numbered) {
    static const char bullet[] = "\xE2\x80\xA2";
    const size_t bullet_len = sizeof(bullet) - SKIP_ONE;
    const char *s = l->s;
    size_t n = l->len;
    go_trim(&s, &n);
    size_t skip = 0;
    *numbered = false;
    if (n > 0 && (s[0] == '*' || s[0] == '+' || s[0] == '-')) {
        skip = SKIP_ONE;
    } else if (go_has_prefix(s, n, bullet)) {
        skip = bullet_len;
    } else {
        while (skip < n && go_ascii_digit((unsigned char)s[skip])) {
            skip++;
        }
        if (skip == 0 || skip >= n || (s[skip] != '.' && s[skip] != ')')) {
            return NULL;
        }
        skip++;
        *numbered = true;
    }
    const char *rest = s + skip;
    size_t rn = n - skip;
    if (rn == 0 || !go_blank_or_tab(rest[0])) {
        return NULL;
    }
    *rest_len = rn;
    return rest; /* n was trimmed, so text follows the white space */
}

static bool go_is_list(const go_line_t *l) {
    size_t rest_len = 0;
    bool numbered = false;
    return go_list_marker(l, &rest_len, &numbered) != NULL;
}

/* go/doc/comment isHeading (trailing white space is already gone). */
static bool go_is_heading(const go_line_t *l) {
    return l->len >= PAIR_LEN && l->s[0] == '#' && go_blank_or_tab(l->s[SKIP_ONE]);
}

static bool go_push_span(go_doc_t *d, int start, int end, int kind) {
    go_span_t *grown =
        (go_span_t *)go_grow(d->tmp, d->spans, d->nspans, &d->spans_cap, sizeof(go_span_t));
    if (!grown) {
        d->failed = true;
        return false;
    }
    d->spans = grown;
    d->spans[d->nspans++] = (go_span_t){.start = start, .end = end, .kind = kind};
    return true;
}

/* An indented (or force-indented) run starting at `start`: a code block or a
 * list. Blank lines belong to it; an unindented list that is being treated as
 * indented takes further unindented list items up to the first blank line. A
 * `}` line right after the run is taken along (pasted, unindented code). */
static int go_indented_span(const go_doc_t *d, int start, int force_indent, int *kind) {
    const go_line_t *L = d->lines;
    bool starts_list = go_is_list(&L[start]);
    bool unindented_list_ok = starts_list && start < force_indent;
    int i = start + SKIP_ONE;
    while (i < d->nlines && (L[i].len == 0 || i < force_indent || go_indented(&L[i]) ||
                             (unindented_list_ok && go_is_list(&L[i])))) {
        if (L[i].len == 0) {
            unindented_list_ok = false;
        }
        i++;
    }
    int end = i;
    while (end > start && L[end - SKIP_ONE].len == 0) {
        end--;
    }
    if (end < d->nlines && L[end].len > 0 && L[end].s[0] == '}') {
        end++;
    }
    *kind = starts_list ? GO_SPAN_LIST : GO_SPAN_CODE;
    return end;
}

static bool go_ends_with(const go_line_t *l, char c) {
    return l->len > 0 && l->s[l->len - SKIP_ONE] == c;
}

/* An unindented run starting at `start`: a paragraph or a heading. When an
 * indented code block follows without a blank line, trailing lines that look
 * like its unindented beginning (list items; a line ending in `{` or `\`)
 * are left for it: *force_indent is then the block's first indented line. */
static int go_unindented_span(const go_doc_t *d, int start, int *force_indent) {
    const go_line_t *L = d->lines;
    int i = start + SKIP_ONE;
    while (i < d->nlines && L[i].len > 0 && !go_indented(&L[i])) {
        i++;
    }
    int end = i;
    if (i < d->nlines && L[i].len > 0 && !go_is_list(&L[i])) {
        if (go_is_list(&L[i - SKIP_ONE])) {
            *force_indent = end;
            end--;
            while (end > start && go_is_list(&L[end - SKIP_ONE])) {
                end--;
            }
        } else if (go_ends_with(&L[i - SKIP_ONE], '{') || go_ends_with(&L[i - SKIP_ONE], '\\')) {
            *force_indent = end;
            end--;
        }
    }
    return end;
}

/* go/doc/comment parseSpans. A line is looked at as unindented text at most
 * once and as part of an indented run at most once. */
static void go_parse_spans(go_doc_t *d) {
    int i = 0;
    int force_indent = 0;
    while (!d->failed) {
        while (i < d->nlines && d->lines[i].len == 0) {
            i++;
        }
        if (i >= d->nlines) {
            return;
        }
        int kind = GO_SPAN_PARA;
        bool indented = i < force_indent || go_indented(&d->lines[i]);
        int end = indented ? go_indented_span(d, i, force_indent, &kind)
                           : go_unindented_span(d, i, &force_indent);
        if (end == i) {
            continue; /* every line was left for the block: now force-indented */
        }
        if (!indented && end == i + SKIP_ONE && go_is_heading(&d->lines[i])) {
            kind = GO_SPAN_HEADING; /* one unindented line: `# Title` */
        }
        if (!go_push_span(d, i, end, kind)) {
            return;
        }
        i = end;
    }
}

/* ── Link definitions ────────────────────────────────────────────── */

static bool go_is_scheme(const char *s, size_t n) {
    static const char *const schemes[] = {"file",  "ftp",    "gopher", "http",
                                          "https", "mailto", "nntp"};
    for (size_t i = 0; i < sizeof(schemes) / sizeof(schemes[0]); i++) {
        if (strlen(schemes[i]) == n && memcmp(schemes[i], s, n) == 0) {
            return true;
        }
    }
    return false;
}

/* Offset of the first occurrence of the two bytes `a b` in s, or n. */
static size_t go_find2(const char *s, size_t n, char a, char b) {
    for (size_t i = 0; i + SKIP_ONE < n; i++) {
        if (s[i] == a && s[i + SKIP_ONE] == b) {
            return i;
        }
    }
    return n;
}

/* go/doc/comment parseLink: `[text]: url` with a known scheme. */
static bool go_parse_linkdef(const go_line_t *l, go_linkdef_t *out) {
    const char *s = l->s;
    size_t n = l->len;
    if (n == 0 || s[0] != '[') {
        return false;
    }
    size_t close = go_find2(s, n, ']', ':');
    if (close + GO_LINKDEF_TAIL >= n || !go_blank_or_tab(s[close + PAIR_LEN])) {
        return false;
    }
    const char *url = s + close + GO_LINKDEF_TAIL;
    size_t url_len = n - close - GO_LINKDEF_TAIL;
    go_trim(&url, &url_len);
    size_t colon = go_find2(url, url_len, ':', '/');
    if (colon + PAIR_LEN >= url_len || url[colon + PAIR_LEN] != '/' || !go_is_scheme(url, colon)) {
        return false;
    }
    out->text = s + SKIP_ONE;
    out->text_len = close - SKIP_ONE;
    out->url = url;
    out->url_len = url_len;
    return true;
}

/* A paragraph whose every line is a link definition defines links and is no
 * text. */
static bool go_para_is_linkdefs(const go_line_t *lines, int n) {
    go_linkdef_t def;
    for (int i = 0; i < n; i++) {
        if (!go_parse_linkdef(&lines[i], &def)) {
            return false;
        }
    }
    return n > 0;
}

typedef void (*go_para_fn)(go_doc_t *d, const go_line_t *lines, int n);

static void go_collect_linkdefs(go_doc_t *d, const go_line_t *lines, int n) {
    if (!go_para_is_linkdefs(lines, n)) {
        return;
    }
    for (int i = 0; i < n && !d->failed; i++) {
        go_linkdef_t def = {0};
        (void)go_parse_linkdef(&lines[i], &def);
        go_linkdef_t *grown =
            (go_linkdef_t *)go_grow(d->tmp, d->defs, d->ndefs, &d->defs_cap, sizeof(go_linkdef_t));
        if (!grown) {
            d->failed = true;
            return;
        }
        d->defs = grown;
        def.order = d->ndefs;
        d->defs[d->ndefs++] = def;
    }
}

/* Byte order of two link texts; the key side reads newline and tab as a
 * space, as parseLinkedText does for the text between the brackets. */
static int go_linktext_cmp(const char *key, size_t key_len, const char *text, size_t text_len,
                           bool fold_key) {
    size_t n = key_len < text_len ? key_len : text_len;
    for (size_t i = 0; i < n; i++) {
        unsigned char k = (unsigned char)key[i];
        if (fold_key && (k == '\n' || k == '\t')) {
            k = ' ';
        }
        unsigned char t = (unsigned char)text[i];
        if (k != t) {
            return k < t ? -SKIP_ONE : SKIP_ONE;
        }
    }
    if (key_len == text_len) {
        return 0;
    }
    return key_len < text_len ? -SKIP_ONE : SKIP_ONE;
}

static int go_linkdef_cmp(const void *a, const void *b) {
    const go_linkdef_t *x = (const go_linkdef_t *)a;
    const go_linkdef_t *y = (const go_linkdef_t *)b;
    int c = go_linktext_cmp(x->text, x->text_len, y->text, y->text_len, false);
    return c ? c : x->order - y->order;
}

/* The first definition of the link text `key`, or NULL. */
static const go_linkdef_t *go_find_linkdef(const go_doc_t *d, const char *key, size_t key_len) {
    int lo = 0;
    int hi = d->ndefs - SKIP_ONE;
    const go_linkdef_t *hit = NULL;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        int c = go_linktext_cmp(key, key_len, d->defs[mid].text, d->defs[mid].text_len, true);
        if (c == 0) {
            hit = &d->defs[mid];
            hi = mid - SKIP_ONE; /* the first of equal texts: defined first */
        } else if (c < 0) {
            hi = mid - SKIP_ONE;
        } else {
            lo = mid + SKIP_ONE;
        }
    }
    return hit;
}

/* ── Bracket scan (parseLinkedText, docLink) ─────────────────────── */

static void go_emit(go_doc_t *d, const char *raw, size_t len, uint32_t line, int syntax) {
    char *copy = cbm_arena_strndup(d->ctx->arena, raw, len);
    if (!copy) {
        d->failed = true;
        return;
    }
    CBMDocLink link = {
        .source_qn = d->source_qn,
        .raw = copy,
        .line = line,
        .def_line = d->def_line,
        .syntax = (uint16_t)syntax,
    };
    cbm_doclinks_push(&d->ctx->result->doc_links, d->ctx->arena, link);
}

/* True when the reference is one word that names a parameter of the
 * documented definition: local, never a reference. */
static bool go_names_own_param(const go_doc_t *d, const CBMDocLinkGoRef *ref) {
    if (!d->params || ref->recv_len > 0 || (ref->pkg_len > 0 && ref->name_len > 0)) {
        return false;
    }
    const char *word = ref->pkg_len > 0 ? ref->pkg : ref->name;
    size_t len = ref->pkg_len > 0 ? ref->pkg_len : ref->name_len;
    for (const char **p = d->params; *p; p++) {
        if (strlen(*p) == len && memcmp(*p, word, len) == 0) {
            return true;
        }
    }
    return false;
}

/* The link definition the pair text[open] .. text[close] names, or NULL.
 * parseLinkedText collects the looked-up text from the FIRST opening bracket
 * since the last closing one and leaves the brackets themselves out: with one
 * opening bracket that is the pair's own text, with `[a [b]` it is `a b`. */
static const go_linkdef_t *go_pair_linkdef(go_doc_t *d, const char *text, size_t first, size_t open,
                                           size_t close) {
    if (d->ndefs == 0) {
        return NULL;
    }
    if (first == open) {
        return go_find_linkdef(d, text + open + SKIP_ONE, close - open - SKIP_ONE);
    }
    char *key = (char *)cbm_arena_alloc(d->tmp, close - first);
    if (!key) {
        d->failed = true;
        return NULL;
    }
    size_t w = 0;
    for (size_t i = first + SKIP_ONE; i < close; i++) {
        if (text[i] != '[') {
            key[w++] = text[i];
        }
    }
    return go_find_linkdef(d, key, w);
}

/* One bracket pair text[open] .. text[close] of a paragraph; `first` is the
 * first opening bracket since the last closing one. */
static void go_bracket(go_doc_t *d, const char *text, size_t len, size_t first, size_t open,
                       size_t close, uint32_t line) {
    const char *inner = text + open + SKIP_ONE;
    size_t inner_len = close - open - SKIP_ONE;
    const go_linkdef_t *def = go_pair_linkdef(d, text, first, open, close);
    if (def) {
        /* a URL is external whatever it says: a long one is stored clipped */
        go_emit(d, def->url, go_clip(def->url, def->url_len, GO_URL_MAX), line, CBM_DOCLINK_HREF);
        return;
    }
    if (open > 0 && !go_border_ok(go_rune_before(text, open))) {
        return;
    }
    if (close + SKIP_ONE < len && !go_border_ok(go_rune_at(text, len, close + SKIP_ONE))) {
        return;
    }
    if (inner_len > GO_REF_MAX) {
        /* Not decided here, and never by its beginning: the resolver reports
         * the clipped text as unparseable. */
        if (go_long_ref_shaped(inner, inner_len)) {
            go_emit(d, inner, go_clip(inner, inner_len, (size_t)GO_REF_MAX + SKIP_ONE), line,
                    CBM_DOCLINK_GO_DOC_LINK);
        }
        return;
    }
    CBMDocLinkGoRef ref;
    if (!cbm_doclink_go_parse_ref(inner, inner_len, &ref) || go_names_own_param(d, &ref)) {
        return;
    }
    go_emit(d, inner, inner_len, line, CBM_DOCLINK_GO_DOC_LINK);
}

/* The paragraph's lines joined by newlines, scanned for bracket pairs: `[`
 * (re)starts a pair, the next `]` closes it. */
static void go_scan_paragraph(go_doc_t *d, const go_line_t *lines, int n) {
    if (go_para_is_linkdefs(lines, n)) {
        return;
    }
    size_t total = 0;
    bool any = false;
    for (int i = 0; i < n; i++) {
        total += (size_t)lines[i].len + SKIP_ONE;
        any = any || memchr(lines[i].s, '[', lines[i].len) != NULL;
    }
    if (!any) {
        return;
    }
    char *text = (char *)cbm_arena_alloc(d->tmp, total + SKIP_ONE);
    if (!text) {
        d->failed = true;
        return;
    }
    size_t len = 0;
    for (int i = 0; i < n; i++) {
        if (i > 0) {
            text[len++] = '\n';
        }
        memcpy(text + len, lines[i].s, lines[i].len);
        len += lines[i].len;
    }
    text[len] = '\0';
    int piece = 0; /* the line the scan is in */
    int open_piece = 0;
    size_t open = len;  /* len: no open bracket */
    size_t first = len; /* the first opening bracket since the last closing one */
    for (size_t i = 0; i < len && !d->failed; i++) {
        char c = text[i];
        if (c == '\n') {
            piece++;
        } else if (c == '[') {
            first = open < len ? first : i;
            open = i;
            open_piece = piece;
        } else if (c == ']') {
            if (open < len) {
                go_bracket(d, text, len, first, open, i, lines[open_piece].line);
            }
            open = len;
        }
    }
}

/* ── Paragraphs of the comment ───────────────────────────────────── */

/* go/doc/comment list(): each item is a run of paragraphs; an item starts at
 * a marker of the list's own kind, a blank line ends a paragraph, and every
 * line is trimmed. */
static void go_list_paragraphs(go_doc_t *d, const go_span_t *sp, go_para_fn fn) {
    size_t rest_len = 0;
    bool numbered = false;
    bool list_numbered = false;
    (void)go_list_marker(&d->lines[sp->start], &rest_len, &list_numbered);
    int count = 0;
    for (int i = sp->start; i < sp->end; i++) {
        go_line_t l = d->lines[i];
        const char *rest = go_list_marker(&l, &rest_len, &numbered);
        bool new_item = rest && numbered == list_numbered;
        if (new_item) {
            l.s = rest;
            l.len = (uint32_t)rest_len;
        }
        const char *s = l.s;
        size_t n = l.len;
        go_trim(&s, &n);
        if ((new_item || n == 0) && count > 0) {
            fn(d, d->para, count);
            count = 0;
        }
        if (n > 0) {
            d->para[count++] = (go_line_t){.s = s, .len = (uint32_t)n, .line = l.line};
        }
    }
    if (count > 0) {
        fn(d, d->para, count);
    }
}

static void go_each_paragraph(go_doc_t *d, go_para_fn fn) {
    for (int i = 0; i < d->nspans && !d->failed; i++) {
        const go_span_t *sp = &d->spans[i];
        if (sp->kind == GO_SPAN_PARA) {
            fn(d, &d->lines[sp->start], sp->end - sp->start);
        } else if (sp->kind == GO_SPAN_LIST) {
            go_list_paragraphs(d, sp, fn);
        }
    }
}

void cbm_doclink_go_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line) {
    if (!doc || !strchr(doc, '[') || !strchr(doc, ']')) {
        return; /* nearly every comment: no bracket, nothing to model */
    }
    go_doc_t d = {.ctx = ctx,
                  .tmp = go_scratch(ctx),
                  .source_qn = def->qualified_name,
                  .params = def->param_names,
                  .def_line = def->start_line};
    go_group_lines(&d, doc, doc_line);
    go_unindent(&d);
    if (d.failed || d.nlines == 0) {
        return;
    }
    d.para = (go_line_t *)cbm_arena_alloc(d.tmp, (size_t)d.nlines * sizeof(go_line_t));
    if (!d.para) {
        return;
    }
    go_parse_spans(&d);
    /* Definitions first: a link may be used before the paragraph defining it. */
    go_each_paragraph(&d, go_collect_linkdefs);
    if (d.ndefs > SKIP_ONE) {
        qsort(d.defs, (size_t)d.ndefs, sizeof(d.defs[0]), go_linkdef_cmp);
    }
    go_each_paragraph(&d, go_scan_paragraph);
}

void cbm_doclink_gomod_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                 uint32_t doc_line) {
    (void)ctx;
    (void)def;
    (void)doc;
    (void)doc_line; /* a go.mod comment is no Go doc comment */
}

/* ── Scope scan ──────────────────────────────────────────────────── */

/* A declaration that claims the flat node name <dir>.<name>: its record gets
 * the w flag once the file's claims of that name are all known. */
typedef struct {
    const char *name;
    size_t name_len;
    uint32_t start; /* byte offset: document order */
    uint32_t row;
    int rec; /* index into recs */
} go_claim_t;

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *tmp;
    char **recs;
    int nrecs;
    int recs_cap;
    go_claim_t *claims;
    int nclaims;
    int claims_cap;
    bool failed;
} go_scan_t;

/* A string that grows in the scratch arena. */
typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} go_sb_t;

static void go_sb_put(go_sb_t *sb, const char *s, size_t n) {
    if (sb->failed) {
        return;
    }
    if (sb->len + n + SKIP_ONE > sb->cap) {
        size_t ncap = sb->cap ? sb->cap : GO_SB_INIT;
        while (ncap < sb->len + n + SKIP_ONE) {
            ncap *= PAIR_LEN;
        }
        char *grown = (char *)cbm_arena_alloc(sb->a, ncap);
        if (!grown) {
            sb->failed = true;
            return;
        }
        if (sb->len > 0) {
            memcpy(grown, sb->buf, sb->len);
        }
        sb->buf = grown;
        sb->cap = ncap;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

/* Append one name to a ','-joined list. */
static void go_sb_name(go_sb_t *sb, const char *s, size_t n) {
    if (sb->len > 0) {
        go_sb_put(sb, ",", SKIP_ONE);
    }
    go_sb_put(sb, s, n);
}

/* The children of a node, one after the other, in linear time (ts_node_child
 * by index is linear per call). After the last child, or after go_kids_end,
 * the cursor is released. */
typedef struct {
    TSTreeCursor cur;
    bool live;
} go_kids_t;

static void go_kids_end(go_kids_t *k) {
    if (k->live) {
        ts_tree_cursor_delete(&k->cur);
        k->live = false;
    }
}

static bool go_kids_first(go_kids_t *k, TSNode parent, TSNode *out) {
    *k = (go_kids_t){0}; /* both are defined on every path, for the caller's loop */
    *out = (TSNode){0};
    if (ts_node_is_null(parent)) {
        return false;
    }
    k->cur = ts_tree_cursor_new(parent);
    k->live = true;
    if (!ts_tree_cursor_goto_first_child(&k->cur)) {
        go_kids_end(k);
        return false;
    }
    *out = ts_tree_cursor_current_node(&k->cur);
    return true;
}

static bool go_kids_next(go_kids_t *k, TSNode *out) {
    if (!k->live || !ts_tree_cursor_goto_next_sibling(&k->cur)) {
        go_kids_end(k);
        return false;
    }
    *out = ts_tree_cursor_current_node(&k->cur);
    return true;
}

/* True when the child the iteration stands on is the parent's `field`. */
static bool go_kid_is_field(go_kids_t *k, const char *field) {
    const char *name = k->live ? ts_tree_cursor_current_field_name(&k->cur) : NULL;
    return name && strcmp(name, field) == 0;
}

static const char *go_node_span(const go_scan_t *s, TSNode n, size_t *len) {
    *len = 0;
    if (ts_node_is_null(n) || !s->ctx->source) {
        return "";
    }
    uint32_t sb = ts_node_start_byte(n);
    uint32_t eb = ts_node_end_byte(n);
    if (eb < sb || eb > (uint32_t)s->ctx->source_len) {
        return "";
    }
    *len = eb - sb;
    return s->ctx->source + sb;
}

static bool go_kind_is(TSNode n, const char *kind) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), kind) == 0;
}

/* The first child of `parent` with this node kind (a null node when none). */
static TSNode go_first_child_of_kind(TSNode parent, const char *kind) {
    TSNode found = {0};
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, parent, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (strcmp(ts_node_type(ch), kind) == 0) {
            found = ch;
            go_kids_end(&k);
        }
    }
    return found;
}

/* The text of an exported identifier node; NULL for anything else. */
static const char *go_exported(const go_scan_t *s, TSNode n, size_t *len) {
    const char *t = go_node_span(s, n, len);
    return go_is_name(t, *len) ? t : NULL;
}

/* Append a record; returns its index, or CBM_NOT_FOUND. */
static int go_push_rec(go_scan_t *s, char *rec) {
    if (!rec) {
        s->failed = true;
        return CBM_NOT_FOUND;
    }
    char **grown = (char **)go_grow(s->tmp, s->recs, s->nrecs, &s->recs_cap, sizeof(char *));
    if (!grown) {
        s->failed = true;
        return CBM_NOT_FOUND;
    }
    s->recs = grown;
    s->recs[s->nrecs] = rec;
    return s->nrecs++;
}

/* The names a type parameter list declares. */
static void go_type_param_names(const go_scan_t *s, TSNode list, go_sb_t *out) {
    go_kids_t decls;
    TSNode decl;
    for (bool ok = go_kids_first(&decls, list, &decl); ok; ok = go_kids_next(&decls, &decl)) {
        if (!go_kind_is(decl, "type_parameter_declaration")) {
            continue;
        }
        go_kids_t ids;
        TSNode id;
        for (bool more = go_kids_first(&ids, decl, &id); more; more = go_kids_next(&ids, &id)) {
            size_t len = 0;
            const char *t = go_node_span(s, id, &len);
            if (go_kind_is(id, "identifier") && go_kid_is_field(&ids, "name") &&
                go_is_ident(t, len)) {
                go_sb_name(out, t, len);
            }
        }
    }
}

/* The type arguments of a generic receiver `(l *List[T, U])` are the method's
 * type parameters. */
static void go_type_arg_names(const go_scan_t *s, TSNode args, go_sb_t *out) {
    go_kids_t k;
    TSNode arg;
    for (bool ok = go_kids_first(&k, args, &arg); ok; ok = go_kids_next(&k, &arg)) {
        TSNode id = go_kind_is(arg, "type_elem") && ts_node_named_child_count(arg) > 0
                        ? ts_node_named_child(arg, 0)
                        : arg;
        size_t len = 0;
        const char *t = go_node_span(s, id, &len);
        if (go_kind_is(id, "type_identifier") && go_is_ident(t, len)) {
            go_sb_name(out, t, len);
        }
    }
}

/* `g first last names`: the type parameters in scope of the declaration at
 * `node`, from its first line to its last (a generic type's fields and method
 * specifications are declared inside it). */
static void go_push_generics(go_scan_t *s, TSNode node, const go_sb_t *names) {
    if (names->failed) {
        s->failed = true;
    } else if (names->len > 0) {
        (void)go_push_rec(s, cbm_arena_sprintf(s->tmp, "g\t%u\t%u\t%s",
                                               ts_node_start_point(node).row + TS_LINE_OFFSET,
                                               ts_node_end_point(node).row + TS_LINE_OFFSET,
                                               names->buf));
    }
}

/* The type parameters a declaration itself lists (`type_parameters`). */
static void go_scan_type_params(go_scan_t *s, TSNode decl) {
    TSNode tparams = ts_node_child_by_field_name(decl, TS_FIELD("type_parameters"));
    if (ts_node_is_null(tparams)) {
        return;
    }
    go_sb_t names = {.a = s->tmp};
    go_type_param_names(s, tparams, &names);
    go_push_generics(s, decl, &names);
}

/* Append the record of a declaration that claims the node <dir>.<name>; its w
 * field is added by go_finish_claims. `at` is the declaration's node. */
static void go_push_claim(go_scan_t *s, char *rec, const char *name, size_t len, TSNode at) {
    int idx = go_push_rec(s, rec);
    go_claim_t *grown =
        (go_claim_t *)go_grow(s->tmp, s->claims, s->nclaims, &s->claims_cap, sizeof(go_claim_t));
    if (idx < 0 || !grown) {
        s->failed = true;
        return;
    }
    s->claims = grown;
    s->claims[s->nclaims++] = (go_claim_t){.name = name,
                                           .name_len = len,
                                           .start = ts_node_start_byte(at),
                                           .row = ts_node_start_point(at).row,
                                           .rec = idx};
}

static void go_scan_func(go_scan_t *s, TSNode fn) {
    go_scan_type_params(s, fn);
    size_t len = 0;
    const char *name = go_exported(s, ts_node_child_by_field_name(fn, TS_FIELD("name")), &len);
    if (name) {
        go_push_claim(s, cbm_arena_sprintf(s->tmp, "F\t%.*s", (int)len, name), name, len, fn);
    }
}

/* The receiver's base type name (as extract_defs.c go_receiver_type_name:
 * through pointer_type and generic_type); its type arguments go to
 * `generics`. NULL when the receiver type is no plain name. */
static const char *go_receiver_type(const go_scan_t *s, TSNode recv, size_t *len,
                                    go_sb_t *generics) {
    TSNode decl = go_first_child_of_kind(recv, "parameter_declaration");
    if (ts_node_is_null(decl)) {
        return NULL;
    }
    TSNode tn = ts_node_child_by_field_name(decl, TS_FIELD("type"));
    for (int guard = 0; guard < GO_RECV_UNWRAP && !ts_node_is_null(tn); guard++) {
        if (go_kind_is(tn, "type_identifier")) {
            const char *t = go_node_span(s, tn, len);
            return go_is_ident(t, *len) ? t : NULL;
        }
        if (!go_kind_is(tn, "pointer_type") && !go_kind_is(tn, "generic_type")) {
            return NULL;
        }
        TSNode targs = ts_node_child_by_field_name(tn, TS_FIELD("type_arguments"));
        if (!ts_node_is_null(targs)) {
            go_type_arg_names(s, targs, generics);
        }
        TSNode inner = ts_node_child_by_field_name(tn, TS_FIELD("type"));
        tn = ts_node_is_null(inner) ? ts_node_named_child(tn, 0) : inner;
    }
    return NULL;
}

static void go_scan_method(go_scan_t *s, TSNode m) {
    go_sb_t generics = {.a = s->tmp};
    size_t recv_len = 0;
    const char *recv = go_receiver_type(s, ts_node_child_by_field_name(m, TS_FIELD("receiver")),
                                        &recv_len, &generics);
    go_push_generics(s, m, &generics);
    size_t len = 0;
    const char *name = go_exported(s, ts_node_child_by_field_name(m, TS_FIELD("name")), &len);
    if (!name) {
        return;
    }
    if (!recv) {
        recv = "?";
        recv_len = SKIP_ONE;
    }
    go_push_claim(s,
                  cbm_arena_sprintf(s->tmp, "M\t%.*s\t%.*s", (int)recv_len, recv, (int)len, name),
                  name, len, m);
}

/* The implied name of an embedded field: the type's own name, without
 * pointer, package qualifier and type arguments. */
static const char *go_embedded_name(const go_scan_t *s, TSNode field, size_t *len) {
    TSNode tn = ts_node_child_by_field_name(field, TS_FIELD("type"));
    for (int guard = 0; guard < GO_EMBED_UNWRAP && !ts_node_is_null(tn); guard++) {
        if (go_kind_is(tn, "type_identifier")) {
            return go_exported(s, tn, len);
        }
        if (go_kind_is(tn, "qualified_type")) {
            tn = ts_node_child_by_field_name(tn, TS_FIELD("name"));
        } else if (go_kind_is(tn, "generic_type") || go_kind_is(tn, "pointer_type")) {
            TSNode inner = ts_node_child_by_field_name(tn, TS_FIELD("type"));
            tn = ts_node_is_null(inner) ? ts_node_named_child(tn, 0) : inner;
        } else {
            return NULL;
        }
    }
    return NULL;
}

static void go_push_field(go_scan_t *s, const char *type, size_t type_len, const char *name,
                          size_t len, bool node) {
    (void)go_push_rec(s, cbm_arena_sprintf(s->tmp, "f\t%.*s\t%.*s\t%d", (int)type_len, type,
                                           (int)len, name, node ? GO_NODE_FLAG : 0));
}

/* `f type name node` for every exported name of one field declaration. The
 * extractor gives a node to the declaration's first name only. */
static void go_scan_field(go_scan_t *s, const char *type, size_t type_len, TSNode field) {
    bool first = true;
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, field, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (!go_kid_is_field(&k, "name")) {
            continue;
        }
        size_t len = 0;
        const char *name = go_exported(s, ch, &len);
        if (name) {
            go_push_field(s, type, type_len, name, len, first);
        }
        first = false;
    }
    if (first) { /* no name: an embedded field */
        size_t len = 0;
        const char *name = go_embedded_name(s, field, &len);
        if (name) {
            go_push_field(s, type, type_len, name, len, false);
        }
    }
}

/* The fields of a struct type / the methods of an interface type. */
static void go_scan_members(go_scan_t *s, const char *type, size_t type_len, TSNode body) {
    TSNode list = go_kind_is(body, "struct_type")
                      ? go_first_child_of_kind(body, "field_declaration_list")
                      : body;
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, list, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (go_kind_is(ch, "field_declaration")) {
            go_scan_field(s, type, type_len, ch);
        } else if (go_kind_is(ch, "method_elem")) {
            size_t len = 0;
            const char *name =
                go_exported(s, ts_node_child_by_field_name(ch, TS_FIELD("name")), &len);
            if (name) {
                (void)go_push_rec(s, cbm_arena_sprintf(s->tmp, "m\t%.*s\t%.*s", (int)type_len, type,
                                                       (int)len, name));
            }
        }
    }
}

/* The target of `type X = Y`: "Y" or "pkg.Y" (type arguments dropped); "-"
 * for anything that is no named type. */
static const char *go_alias_target(go_scan_t *s, TSNode type) {
    TSNode tn = go_kind_is(type, "generic_type")
                    ? ts_node_child_by_field_name(type, TS_FIELD("type"))
                    : type;
    size_t len = 0;
    const char *target = "-";
    if (go_kind_is(tn, "type_identifier")) {
        const char *t = go_exported(s, tn, &len);
        target = t ? cbm_arena_strndup(s->tmp, t, len) : "-";
    } else if (go_kind_is(tn, "qualified_type")) {
        size_t pkg_len = 0;
        const char *pkg =
            go_node_span(s, ts_node_child_by_field_name(tn, TS_FIELD("package")), &pkg_len);
        const char *name = go_exported(s, ts_node_child_by_field_name(tn, TS_FIELD("name")), &len);
        if (name && go_is_ident(pkg, pkg_len)) {
            target = cbm_arena_sprintf(s->tmp, "%.*s.%.*s", (int)pkg_len, pkg, (int)len, name);
        }
    }
    s->failed = s->failed || !target;
    return target ? target : "-";
}

/* One type_spec or type_alias. */
static void go_scan_type(go_scan_t *s, TSNode spec) {
    go_scan_type_params(s, spec);
    size_t len = 0;
    const char *name = go_exported(s, ts_node_child_by_field_name(spec, TS_FIELD("name")), &len);
    if (!name) {
        return;
    }
    TSNode type = ts_node_child_by_field_name(spec, TS_FIELD("type"));
    char kind = 'o';
    const char *alias = "-";
    if (go_kind_is(spec, "type_alias")) {
        kind = 'a';
        alias = go_alias_target(s, type);
    } else if (go_kind_is(type, "struct_type")) {
        kind = 's';
    } else if (go_kind_is(type, "interface_type")) {
        kind = 'i';
    }
    go_push_claim(s, cbm_arena_sprintf(s->tmp, "T\t%.*s\t%c\t%s", (int)len, name, kind, alias),
                  name, len, spec);
    if (kind == 's' || kind == 'i') {
        go_scan_members(s, name, len, type);
    }
}

static void go_scan_type_decl(go_scan_t *s, TSNode decl) {
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, decl, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (go_kind_is(ch, "type_spec") || go_kind_is(ch, "type_alias")) {
            go_scan_type(s, ch);
        }
    }
}

/* `V name w` for every exported name of a const_spec / var_spec. The extractor
 * gives a node to the FIRST name of a spec that is a direct child of its
 * declaration (every constant spec; a `var` without parentheses): that name
 * claims <dir>.<name>, the others have w `-`. */
static void go_scan_value_spec(go_scan_t *s, TSNode spec, bool spec_has_node) {
    bool first = true;
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, spec, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (!go_kid_is_field(&k, "name")) {
            continue;
        }
        size_t len = 0;
        const char *name = go_exported(s, ch, &len);
        if (name && first && spec_has_node) {
            go_push_claim(s, cbm_arena_sprintf(s->tmp, "V\t%.*s", (int)len, name), name, len, spec);
        } else if (name) {
            (void)go_push_rec(s, cbm_arena_sprintf(s->tmp, "V\t%.*s\t-", (int)len, name));
        }
        first = false;
    }
}

/* The specs of a parenthesized `var ( ... )`: none of them has a node. */
static void go_scan_var_list(go_scan_t *s, TSNode list) {
    go_kids_t k;
    TSNode spec;
    for (bool ok = go_kids_first(&k, list, &spec); ok; ok = go_kids_next(&k, &spec)) {
        if (go_kind_is(spec, "var_spec")) {
            go_scan_value_spec(s, spec, false);
        }
    }
}

static void go_scan_value_decl(go_scan_t *s, TSNode decl) {
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, decl, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (go_kind_is(ch, "const_spec") || go_kind_is(ch, "var_spec")) {
            go_scan_value_spec(s, ch, true);
        } else if (go_kind_is(ch, "var_spec_list")) {
            go_scan_var_list(s, ch);
        }
    }
}

/* The content of an import path literal; NULL when it is none this scan can
 * record (not a plain string, empty, too long, or with bytes no path has). */
static const char *go_import_path(const go_scan_t *s, TSNode lit, size_t *len) {
    const char *t = go_node_span(s, lit, len);
    if (*len < PAIR_LEN || (t[0] != '"' && t[0] != '`') || t[*len - SKIP_ONE] != t[0]) {
        return NULL;
    }
    t++;
    *len -= PAIR_LEN;
    if (*len == 0 || *len > GO_PATH_MAX) {
        return NULL;
    }
    for (size_t i = 0; i < *len; i++) {
        unsigned char c = (unsigned char)t[i];
        if (c <= ' ' || c == '\\' || c == '"' || c == '`' || c == GO_ASCII_DEL) {
            return NULL;
        }
    }
    return t;
}

static void go_scan_import_spec(go_scan_t *s, TSNode spec) {
    size_t path_len = 0;
    const char *path =
        go_import_path(s, ts_node_child_by_field_name(spec, TS_FIELD("path")), &path_len);
    TSNode name = ts_node_child_by_field_name(spec, TS_FIELD("name"));
    size_t len = 0;
    const char *local = go_node_span(s, name, &len);
    if (ts_node_is_null(name)) {
        local = "-";
        len = SKIP_ONE;
    } else if (!go_kind_is(name, "package_identifier") || !go_is_ident(local, len) ||
               (len == SKIP_ONE && local[0] == '_')) {
        return; /* `.` and `_` imports give no qualifier */
    }
    if (!path && ts_node_is_null(name)) {
        return; /* neither a name nor a path to derive one from */
    }
    if (!path) {
        path = "?"; /* the qualifier exists; where it leads is not known */
        path_len = SKIP_ONE;
    }
    (void)go_push_rec(
        s, cbm_arena_sprintf(s->tmp, "I\t%.*s\t%.*s", (int)len, local, (int)path_len, path));
}

static void go_scan_import_list(go_scan_t *s, TSNode list) {
    go_kids_t k;
    TSNode spec;
    for (bool ok = go_kids_first(&k, list, &spec); ok; ok = go_kids_next(&k, &spec)) {
        if (go_kind_is(spec, "import_spec")) {
            go_scan_import_spec(s, spec);
        }
    }
}

static void go_scan_imports(go_scan_t *s, TSNode decl) {
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, decl, &ch); ok; ok = go_kids_next(&k, &ch)) {
        if (go_kind_is(ch, "import_spec")) {
            go_scan_import_spec(s, ch);
        } else if (go_kind_is(ch, "import_spec_list")) {
            go_scan_import_list(s, ch);
        }
    }
}

/* A `//go:build` or `// +build` line: the file is built only under a
 * constraint. *ignore is set when the constraint is the word `ignore`: such a
 * file is in no package. */
static bool go_comment_is_build_line(const go_scan_t *s, TSNode comment, bool *ignore) {
    size_t n = 0;
    const char *t = go_node_span(s, comment, &n);
    static const char *const forms[] = {"//go:build", "// +build", "//+build"};
    static const char word[] = "ignore";
    for (size_t i = 0; i < sizeof(forms) / sizeof(forms[0]); i++) {
        size_t fl = strlen(forms[i]);
        if (!go_has_prefix(t, n, forms[i]) || (n > fl && !go_space(t[fl]))) {
            continue;
        }
        const char *rest = t + fl;
        size_t rn = n - fl;
        go_trim(&rest, &rn);
        *ignore = *ignore || (rn == sizeof(word) - SKIP_ONE && memcmp(rest, word, rn) == 0);
        return true;
    }
    return false;
}

/* A declaration that names nodes wherever the extractor's definition walk
 * finds it. */
static bool go_scan_decl(go_scan_t *s, TSNode n) {
    if (go_kind_is(n, "function_declaration")) {
        go_scan_func(s, n);
    } else if (go_kind_is(n, "method_declaration")) {
        go_scan_method(s, n);
    } else if (go_kind_is(n, "type_declaration")) {
        go_scan_type_decl(s, n);
    } else {
        return false;
    }
    return true;
}

/* Push the children of `n` on the walk's stack. */
static TSNode *go_push_children(go_scan_t *s, TSNode n, TSNode *stack, int *top, int *cap) {
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, n, &ch); ok; ok = go_kids_next(&k, &ch)) {
        TSNode *grown = (TSNode *)go_grow(s->tmp, stack, *top, cap, sizeof(TSNode));
        if (!grown) {
            s->failed = true;
            go_kids_end(&k);
            continue;
        }
        stack = grown;
        stack[(*top)++] = ch;
    }
    return stack;
}

/* A tree with parse errors: functions, methods and types are found wherever
 * the extractor's walk finds them (it descends into everything but function
 * nodes), so that this scan and the graph agree on what is declared. An
 * explicit stack: the depth is the input's. */
static void go_scan_error_tree(go_scan_t *s, TSNode root) {
    int cap = 0;
    int top = 0;
    TSNode *stack = (TSNode *)go_grow(s->tmp, NULL, 0, &cap, sizeof(TSNode));
    if (!stack) {
        s->failed = true;
        return;
    }
    stack[top++] = root;
    while (top > 0 && !s->failed) {
        TSNode n = stack[--top];
        if (go_scan_decl(s, n) || go_kind_is(n, "method_elem") || go_kind_is(n, "func_literal")) {
            continue;
        }
        stack = go_push_children(s, n, stack, &top, &cap);
    }
}

static int go_claim_cmp(const void *a, const void *b) {
    const go_claim_t *x = (const go_claim_t *)a;
    const go_claim_t *y = (const go_claim_t *)b;
    size_t n = x->name_len < y->name_len ? x->name_len : y->name_len;
    int c = memcmp(x->name, y->name, n);
    if (c == 0 && x->name_len != y->name_len) {
        c = x->name_len < y->name_len ? -SKIP_ONE : SKIP_ONE;
    }
    if (c != 0) {
        return c;
    }
    return (x->start > y->start) - (x->start < y->start);
}

static bool go_claim_same_name(const go_claim_t *a, const go_claim_t *b) {
    return a->name_len == b->name_len && memcmp(a->name, b->name, a->name_len) == 0;
}

/* The w flag of the claims [first, end) of one name, in text order: they share
 * one node name, and the graph keeps the declaration that starts on the LAST
 * line (cbm_gbuf_upsert_node: largest start_line). Two on that line:
 * contested. */
static void go_flag_claims(go_scan_t *s, int first, int end) {
    uint32_t last_row = s->claims[end - SKIP_ONE].row;
    bool contested = end - first > SKIP_ONE && s->claims[end - PAIR_LEN].row == last_row;
    for (int k = first; k < end; k++) {
        char w = '0';
        if (contested && s->claims[k].row == last_row) {
            w = 'x';
        } else if (!contested && k == end - SKIP_ONE) {
            w = '1';
        }
        int rec = s->claims[k].rec;
        s->recs[rec] = cbm_arena_sprintf(s->tmp, "%s\t%c", s->recs[rec], w);
        s->failed = s->failed || !s->recs[rec];
    }
}

static void go_finish_claims(go_scan_t *s) {
    if (s->nclaims > SKIP_ONE) {
        qsort(s->claims, (size_t)s->nclaims, sizeof(s->claims[0]), go_claim_cmp);
    }
    int i = 0;
    while (i < s->nclaims && !s->failed) {
        int end = i + SKIP_ONE;
        while (end < s->nclaims && go_claim_same_name(&s->claims[i], &s->claims[end])) {
            end++;
        }
        go_flag_claims(s, i, end);
        i = end;
    }
}

static int go_rec_cmp(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

/* "go1" and the sorted, de-duplicated records, in the result arena. */
static const char *go_blob(go_scan_t *s) {
    if (s->failed) {
        return NULL;
    }
    if (s->nrecs > SKIP_ONE) {
        qsort(s->recs, (size_t)s->nrecs, sizeof(s->recs[0]), go_rec_cmp);
    }
    size_t total = sizeof(CBM_DOCLINK_GO_SCOPE_TAG);
    for (int i = 0; i < s->nrecs; i++) {
        total += strlen(s->recs[i]) + SKIP_ONE;
    }
    char *blob = (char *)cbm_arena_alloc(s->ctx->arena, total + SKIP_ONE);
    if (!blob) {
        return NULL;
    }
    size_t w = sizeof(CBM_DOCLINK_GO_SCOPE_TAG) - SKIP_ONE;
    memcpy(blob, CBM_DOCLINK_GO_SCOPE_TAG, w);
    for (int i = 0; i < s->nrecs; i++) {
        if (i > 0 && strcmp(s->recs[i], s->recs[i - SKIP_ONE]) == 0) {
            continue;
        }
        size_t len = strlen(s->recs[i]);
        blob[w++] = '\n';
        memcpy(blob + w, s->recs[i], len);
        w += len;
    }
    blob[w] = '\0';
    return blob;
}

/* What the file's head says about its package. */
typedef struct {
    const char *pkg;
    size_t pkg_len;
    bool seen_package;
    bool ignored;
    bool constrained;
    bool has_error;
} go_head_t;

/* One child of the source file. Values are declared at this level only (the
 * extractor reads variables and constants from the root's children); the
 * other declarations are taken here when the tree has no parse error. */
static void go_scan_top(go_scan_t *s, TSNode ch, go_head_t *h) {
    if (go_kind_is(ch, "comment")) {
        if (!h->seen_package && go_comment_is_build_line(s, ch, &h->ignored)) {
            h->constrained = true;
        }
    } else if (go_kind_is(ch, "package_clause")) {
        h->seen_package = true;
        if (!h->pkg && ts_node_named_child_count(ch) > 0) {
            const char *t = go_node_span(s, ts_node_named_child(ch, 0), &h->pkg_len);
            h->pkg = go_is_ident(t, h->pkg_len) ? t : NULL;
        }
    } else if (go_kind_is(ch, "import_declaration")) {
        go_scan_imports(s, ch);
    } else if (go_kind_is(ch, "const_declaration") || go_kind_is(ch, "var_declaration")) {
        go_scan_value_decl(s, ch);
    } else if (!h->has_error) {
        (void)go_scan_decl(s, ch);
    }
}

enum { GO_HEAD_FLAGS = 4 }; /* i, c, e and the terminator */

/* The flags field of the P record: `-` when none is set. */
static void go_head_flags(const go_head_t *h, char out[GO_HEAD_FLAGS]) {
    size_t n = 0;
    if (h->ignored) {
        out[n++] = 'i';
    }
    if (h->constrained && !h->ignored) {
        out[n++] = 'c';
    }
    if (h->has_error) {
        out[n++] = 'e';
    }
    if (n == 0) {
        out[n++] = '-';
    }
    out[n] = '\0';
}

const char *cbm_doclink_go_scan_scope(CBMExtractCtx *ctx) {
    go_scan_t s = {.ctx = ctx, .tmp = go_scratch(ctx)};
    go_head_t h = {.has_error = ts_node_has_error(ctx->root)};
    go_kids_t k;
    TSNode ch;
    for (bool ok = go_kids_first(&k, ctx->root, &ch); ok; ok = go_kids_next(&k, &ch)) {
        go_scan_top(&s, ch, &h);
    }
    if (h.has_error) {
        go_scan_error_tree(&s, ctx->root);
    }
    if (h.pkg) {
        char flags[GO_HEAD_FLAGS];
        go_head_flags(&h, flags);
        (void)go_push_rec(&s,
                          cbm_arena_sprintf(s.tmp, "P\t%.*s\t%s", (int)h.pkg_len, h.pkg, flags));
    }
    go_finish_claims(&s);
    return go_blob(&s);
}

/* The module path of a go.mod file: `G path`. */
const char *cbm_doclink_gomod_scan_scope(CBMExtractCtx *ctx) {
    go_scan_t s = {.ctx = ctx, .tmp = go_scratch(ctx)};
    TSNode directive = go_first_child_of_kind(ctx->root, "module_directive");
    TSNode path = go_first_child_of_kind(directive, "module_path");
    size_t len = 0;
    const char *t = go_node_span(&s, path, &len);
    if (len >= PAIR_LEN && (t[0] == '"' || t[0] == '`') && t[len - SKIP_ONE] == t[0]) {
        t++;
        len -= PAIR_LEN;
    }
    if (go_valid_import_path(t, len)) {
        (void)go_push_rec(&s, cbm_arena_sprintf(s.tmp, "G\t%.*s", (int)len, t));
    }
    return go_blob(&s);
}

/* The persisted scope: the blob without its `g` lines (the only records that
 * carry a line number, and the only ones no other file reads). */
char *cbm_doclink_go_portable_scope(const char *scope) {
    size_t n = strlen(scope);
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    for (const char *p = scope; *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (!(len >= PAIR_LEN && p[0] == 'g' && p[SKIP_ONE] == '\t')) {
            if (w > 0) {
                out[w++] = '\n';
            }
            memcpy(out + w, p, len);
            w += len;
        }
        p = nl ? nl + SKIP_ONE : p + len;
    }
    out[w] = '\0';
    return out;
}
