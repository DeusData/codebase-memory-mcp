#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#ifdef _MSC_VER
#pragma optimize("", off)
#elif defined(__clang__)
#pragma clang optimize off
#elif defined(__GNUC__)
#pragma GCC optimize ("O0")
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 143
#define LARGE_STATE_COUNT 61
#define SYMBOL_COUNT 56
#define ALIAS_COUNT 0
#define TOKEN_COUNT 33
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 0
#define MAX_ALIAS_SEQUENCE_LENGTH 3
#define PRODUCTION_ID_COUNT 1

enum ts_symbol_identifiers {
  aux_sym__intertoken_token1 = 1,
  aux_sym_comment_token1 = 2,
  anon_sym_POUND_SEMI = 3,
  anon_sym_POUND_BANG = 4,
  aux_sym_directive_token1 = 5,
  anon_sym_POUND_PIPE = 6,
  aux_sym_block_comment_token1 = 7,
  anon_sym_PIPE_POUND = 8,
  sym_boolean = 9,
  sym_number = 10,
  sym_character = 11,
  anon_sym_DQUOTE = 12,
  aux_sym_string_token1 = 13,
  sym_escape_sequence = 14,
  sym_symbol = 15,
  sym_keyword = 16,
  anon_sym_LPAREN = 17,
  anon_sym_RPAREN = 18,
  anon_sym_LBRACK = 19,
  anon_sym_RBRACK = 20,
  anon_sym_LBRACE = 21,
  anon_sym_RBRACE = 22,
  anon_sym_SQUOTE = 23,
  anon_sym_BQUOTE = 24,
  anon_sym_POUND_SQUOTE = 25,
  anon_sym_POUND_BQUOTE = 26,
  anon_sym_COMMA = 27,
  anon_sym_COMMA_AT = 28,
  anon_sym_POUND_COMMA = 29,
  anon_sym_POUND_COMMA_AT = 30,
  anon_sym_POUND_LPAREN = 31,
  anon_sym_POUNDvu8_LPAREN = 32,
  sym_program = 33,
  sym__token = 34,
  sym__intertoken = 35,
  sym_comment = 36,
  sym_directive = 37,
  sym_block_comment = 38,
  sym__datum = 39,
  sym_string = 40,
  sym_list = 41,
  sym_quote = 42,
  sym_quasiquote = 43,
  sym_syntax = 44,
  sym_quasisyntax = 45,
  sym_unquote = 46,
  sym_unquote_splicing = 47,
  sym_unsyntax = 48,
  sym_unsyntax_splicing = 49,
  sym_vector = 50,
  sym_byte_vector = 51,
  aux_sym_program_repeat1 = 52,
  aux_sym_comment_repeat1 = 53,
  aux_sym_block_comment_repeat1 = 54,
  aux_sym_string_repeat1 = 55,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [aux_sym__intertoken_token1] = "_intertoken_token1",
  [aux_sym_comment_token1] = "comment_token1",
  [anon_sym_POUND_SEMI] = "#;",
  [anon_sym_POUND_BANG] = "#!",
  [aux_sym_directive_token1] = "directive_token1",
  [anon_sym_POUND_PIPE] = "#|",
  [aux_sym_block_comment_token1] = "block_comment_token1",
  [anon_sym_PIPE_POUND] = "|#",
  [sym_boolean] = "boolean",
  [sym_number] = "number",
  [sym_character] = "character",
  [anon_sym_DQUOTE] = "\"",
  [aux_sym_string_token1] = "string_token1",
  [sym_escape_sequence] = "escape_sequence",
  [sym_symbol] = "symbol",
  [sym_keyword] = "keyword",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [anon_sym_LBRACK] = "[",
  [anon_sym_RBRACK] = "]",
  [anon_sym_LBRACE] = "{",
  [anon_sym_RBRACE] = "}",
  [anon_sym_SQUOTE] = "'",
  [anon_sym_BQUOTE] = "`",
  [anon_sym_POUND_SQUOTE] = "#'",
  [anon_sym_POUND_BQUOTE] = "#`",
  [anon_sym_COMMA] = ",",
  [anon_sym_COMMA_AT] = ",@",
  [anon_sym_POUND_COMMA] = "#,",
  [anon_sym_POUND_COMMA_AT] = "#,@",
  [anon_sym_POUND_LPAREN] = "#(",
  [anon_sym_POUNDvu8_LPAREN] = "#vu8(",
  [sym_program] = "program",
  [sym__token] = "_token",
  [sym__intertoken] = "_intertoken",
  [sym_comment] = "comment",
  [sym_directive] = "directive",
  [sym_block_comment] = "block_comment",
  [sym__datum] = "_datum",
  [sym_string] = "string",
  [sym_list] = "list",
  [sym_quote] = "quote",
  [sym_quasiquote] = "quasiquote",
  [sym_syntax] = "syntax",
  [sym_quasisyntax] = "quasisyntax",
  [sym_unquote] = "unquote",
  [sym_unquote_splicing] = "unquote_splicing",
  [sym_unsyntax] = "unsyntax",
  [sym_unsyntax_splicing] = "unsyntax_splicing",
  [sym_vector] = "vector",
  [sym_byte_vector] = "byte_vector",
  [aux_sym_program_repeat1] = "program_repeat1",
  [aux_sym_comment_repeat1] = "comment_repeat1",
  [aux_sym_block_comment_repeat1] = "block_comment_repeat1",
  [aux_sym_string_repeat1] = "string_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [aux_sym__intertoken_token1] = aux_sym__intertoken_token1,
  [aux_sym_comment_token1] = aux_sym_comment_token1,
  [anon_sym_POUND_SEMI] = anon_sym_POUND_SEMI,
  [anon_sym_POUND_BANG] = anon_sym_POUND_BANG,
  [aux_sym_directive_token1] = aux_sym_directive_token1,
  [anon_sym_POUND_PIPE] = anon_sym_POUND_PIPE,
  [aux_sym_block_comment_token1] = aux_sym_block_comment_token1,
  [anon_sym_PIPE_POUND] = anon_sym_PIPE_POUND,
  [sym_boolean] = sym_boolean,
  [sym_number] = sym_number,
  [sym_character] = sym_character,
  [anon_sym_DQUOTE] = anon_sym_DQUOTE,
  [aux_sym_string_token1] = aux_sym_string_token1,
  [sym_escape_sequence] = sym_escape_sequence,
  [sym_symbol] = sym_symbol,
  [sym_keyword] = sym_keyword,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_LBRACE] = anon_sym_LBRACE,
  [anon_sym_RBRACE] = anon_sym_RBRACE,
  [anon_sym_SQUOTE] = anon_sym_SQUOTE,
  [anon_sym_BQUOTE] = anon_sym_BQUOTE,
  [anon_sym_POUND_SQUOTE] = anon_sym_POUND_SQUOTE,
  [anon_sym_POUND_BQUOTE] = anon_sym_POUND_BQUOTE,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [anon_sym_COMMA_AT] = anon_sym_COMMA_AT,
  [anon_sym_POUND_COMMA] = anon_sym_POUND_COMMA,
  [anon_sym_POUND_COMMA_AT] = anon_sym_POUND_COMMA_AT,
  [anon_sym_POUND_LPAREN] = anon_sym_POUND_LPAREN,
  [anon_sym_POUNDvu8_LPAREN] = anon_sym_POUNDvu8_LPAREN,
  [sym_program] = sym_program,
  [sym__token] = sym__token,
  [sym__intertoken] = sym__intertoken,
  [sym_comment] = sym_comment,
  [sym_directive] = sym_directive,
  [sym_block_comment] = sym_block_comment,
  [sym__datum] = sym__datum,
  [sym_string] = sym_string,
  [sym_list] = sym_list,
  [sym_quote] = sym_quote,
  [sym_quasiquote] = sym_quasiquote,
  [sym_syntax] = sym_syntax,
  [sym_quasisyntax] = sym_quasisyntax,
  [sym_unquote] = sym_unquote,
  [sym_unquote_splicing] = sym_unquote_splicing,
  [sym_unsyntax] = sym_unsyntax,
  [sym_unsyntax_splicing] = sym_unsyntax_splicing,
  [sym_vector] = sym_vector,
  [sym_byte_vector] = sym_byte_vector,
  [aux_sym_program_repeat1] = aux_sym_program_repeat1,
  [aux_sym_comment_repeat1] = aux_sym_comment_repeat1,
  [aux_sym_block_comment_repeat1] = aux_sym_block_comment_repeat1,
  [aux_sym_string_repeat1] = aux_sym_string_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [aux_sym__intertoken_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_comment_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_POUND_SEMI] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_BANG] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_directive_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_POUND_PIPE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_block_comment_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_PIPE_POUND] = {
    .visible = true,
    .named = false,
  },
  [sym_boolean] = {
    .visible = true,
    .named = true,
  },
  [sym_number] = {
    .visible = true,
    .named = true,
  },
  [sym_character] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_string_token1] = {
    .visible = false,
    .named = false,
  },
  [sym_escape_sequence] = {
    .visible = true,
    .named = true,
  },
  [sym_symbol] = {
    .visible = true,
    .named = true,
  },
  [sym_keyword] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SQUOTE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_BQUOTE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_SQUOTE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_BQUOTE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA_AT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_COMMA_AT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUNDvu8_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [sym_program] = {
    .visible = true,
    .named = true,
  },
  [sym__token] = {
    .visible = false,
    .named = true,
  },
  [sym__intertoken] = {
    .visible = false,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_directive] = {
    .visible = true,
    .named = true,
  },
  [sym_block_comment] = {
    .visible = true,
    .named = true,
  },
  [sym__datum] = {
    .visible = false,
    .named = true,
  },
  [sym_string] = {
    .visible = true,
    .named = true,
  },
  [sym_list] = {
    .visible = true,
    .named = true,
  },
  [sym_quote] = {
    .visible = true,
    .named = true,
  },
  [sym_quasiquote] = {
    .visible = true,
    .named = true,
  },
  [sym_syntax] = {
    .visible = true,
    .named = true,
  },
  [sym_quasisyntax] = {
    .visible = true,
    .named = true,
  },
  [sym_unquote] = {
    .visible = true,
    .named = true,
  },
  [sym_unquote_splicing] = {
    .visible = true,
    .named = true,
  },
  [sym_unsyntax] = {
    .visible = true,
    .named = true,
  },
  [sym_unsyntax_splicing] = {
    .visible = true,
    .named = true,
  },
  [sym_vector] = {
    .visible = true,
    .named = true,
  },
  [sym_byte_vector] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_program_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_comment_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_block_comment_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_string_repeat1] = {
    .visible = false,
    .named = false,
  },
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
};

static const uint16_t ts_non_terminal_alias_map[] = {
  0,
};

static const TSStateId ts_primary_state_ids[STATE_COUNT] = {
  [0] = 0,
  [1] = 1,
  [2] = 2,
  [3] = 3,
  [4] = 4,
  [5] = 5,
  [6] = 6,
  [7] = 7,
  [8] = 8,
  [9] = 9,
  [10] = 10,
  [11] = 11,
  [12] = 4,
  [13] = 13,
  [14] = 11,
  [15] = 13,
  [16] = 16,
  [17] = 5,
  [18] = 6,
  [19] = 8,
  [20] = 9,
  [21] = 10,
  [22] = 3,
  [23] = 16,
  [24] = 24,
  [25] = 25,
  [26] = 26,
  [27] = 24,
  [28] = 28,
  [29] = 29,
  [30] = 30,
  [31] = 31,
  [32] = 28,
  [33] = 29,
  [34] = 34,
  [35] = 35,
  [36] = 36,
  [37] = 37,
  [38] = 31,
  [39] = 39,
  [40] = 40,
  [41] = 41,
  [42] = 42,
  [43] = 43,
  [44] = 44,
  [45] = 39,
  [46] = 25,
  [47] = 26,
  [48] = 30,
  [49] = 34,
  [50] = 50,
  [51] = 35,
  [52] = 36,
  [53] = 37,
  [54] = 40,
  [55] = 41,
  [56] = 42,
  [57] = 43,
  [58] = 44,
  [59] = 50,
  [60] = 60,
  [61] = 61,
  [62] = 62,
  [63] = 63,
  [64] = 64,
  [65] = 65,
  [66] = 66,
  [67] = 67,
  [68] = 68,
  [69] = 69,
  [70] = 70,
  [71] = 71,
  [72] = 72,
  [73] = 73,
  [74] = 74,
  [75] = 75,
  [76] = 76,
  [77] = 77,
  [78] = 78,
  [79] = 79,
  [80] = 80,
  [81] = 81,
  [82] = 82,
  [83] = 83,
  [84] = 84,
  [85] = 85,
  [86] = 86,
  [87] = 87,
  [88] = 88,
  [89] = 89,
  [90] = 90,
  [91] = 91,
  [92] = 92,
  [93] = 93,
  [94] = 60,
  [95] = 93,
  [96] = 92,
  [97] = 74,
  [98] = 73,
  [99] = 80,
  [100] = 75,
  [101] = 81,
  [102] = 76,
  [103] = 83,
  [104] = 84,
  [105] = 86,
  [106] = 88,
  [107] = 89,
  [108] = 90,
  [109] = 65,
  [110] = 62,
  [111] = 63,
  [112] = 64,
  [113] = 91,
  [114] = 66,
  [115] = 61,
  [116] = 67,
  [117] = 68,
  [118] = 69,
  [119] = 70,
  [120] = 77,
  [121] = 79,
  [122] = 82,
  [123] = 85,
  [124] = 71,
  [125] = 78,
  [126] = 72,
  [127] = 87,
  [128] = 128,
  [129] = 129,
  [130] = 130,
  [131] = 130,
  [132] = 128,
  [133] = 128,
  [134] = 130,
  [135] = 135,
  [136] = 136,
  [137] = 137,
  [138] = 136,
  [139] = 135,
  [140] = 89,
  [141] = 79,
  [142] = 142,
};

static TSCharacterRange aux_sym__intertoken_token1_character_set_1[] = {
  {'\t', '\r'}, {' ', ' '}, {0xa0, 0xa0}, {0x1680, 0x1680}, {0x2000, 0x200a}, {0x2028, 0x2029}, {0x202f, 0x202f}, {0x205f, 0x205f},
  {0x3000, 0x3000},
};

static TSCharacterRange aux_sym_directive_token1_character_set_2[] = {
  {0, 0x08}, {0x0e, 0x1f}, {'!', '!'}, {'$', '&'}, {'*', '+'}, {'-', ':'}, {'<', 'Z'}, {'^', '_'},
  {'a', 'z'}, {'~', 0x9f}, {0xa1, 0x167f}, {0x1681, 0x1fff}, {0x200b, 0x2027}, {0x202a, 0x202e}, {0x2030, 0x205e}, {0x2060, 0x2fff},
  {0x3001, 0x10ffff},
};

static TSCharacterRange sym_escape_sequence_character_set_1[] = {
  {'\t', '\n'}, {' ', ' '}, {0x85, 0x85}, {0xa0, 0xa0}, {0x1680, 0x1680}, {0x2000, 0x200a}, {0x202f, 0x202f}, {0x205f, 0x205f},
  {0x3000, 0x3000},
};

static TSCharacterRange sym_escape_sequence_character_set_2[] = {
  {'\t', '\n'}, {'\r', '\r'}, {' ', ' '}, {0x85, 0x85}, {0xa0, 0xa0}, {0x1680, 0x1680}, {0x2000, 0x200a}, {0x2028, 0x2028},
  {0x202f, 0x202f}, {0x205f, 0x205f}, {0x3000, 0x3000},
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(295);
      ADVANCE_MAP(
        '"', 501,
        '#', 305,
        '\'', 610,
        '(', 604,
        ')', 605,
        ',', 614,
        ';', 297,
        '[', 606,
        ']', 607,
        '`', 611,
        '{', 608,
        '|', 304,
        '}', 609,
      );
      if (set_contains(aux_sym__intertoken_token1_character_set_1, 9, lookahead)) ADVANCE(296);
      if (lookahead != 0) ADVANCE(303);
      END_STATE();
    case 1:
      if (lookahead == '\n') ADVANCE(506);
      if (lookahead == '\r') ADVANCE(505);
      if (lookahead == 'X') ADVANCE(507);
      if (lookahead == 'x') ADVANCE(507);
      if (lookahead == 0x85 ||
          lookahead == 0x2028) ADVANCE(506);
      if (set_contains(sym_escape_sequence_character_set_2, 11, lookahead)) ADVANCE(504);
      if (lookahead != 0) ADVANCE(503);
      END_STATE();
    case 2:
      if (lookahead == '\r') ADVANCE(505);
      if (lookahead == '\n' ||
          lookahead == 0x85 ||
          lookahead == 0x2028) ADVANCE(506);
      if (set_contains(sym_escape_sequence_character_set_2, 11, lookahead)) ADVANCE(2);
      END_STATE();
    case 3:
      ADVANCE_MAP(
        '!', 299,
        '\'', 612,
        '(', 619,
        ',', 617,
        ':', 175,
        ';', 298,
        '\\', 106,
        '`', 613,
        'v', 172,
        '|', 302,
        'B', 314,
        'b', 314,
        'D', 24,
        'd', 24,
        'F', 308,
        'f', 308,
        'O', 368,
        'o', 368,
        'T', 309,
        't', 309,
        'X', 381,
        'x', 381,
        'E', 7,
        'I', 7,
        'e', 7,
        'i', 7,
      );
      END_STATE();
    case 4:
      if (lookahead == '!') ADVANCE(299);
      if (lookahead == ';') ADVANCE(298);
      if (lookahead == '|') ADVANCE(302);
      END_STATE();
    case 5:
      if (lookahead == '"') ADVANCE(501);
      if (lookahead == '\\') ADVANCE(1);
      if (lookahead != 0) ADVANCE(502);
      END_STATE();
    case 6:
      if (lookahead == '#') ADVANCE(305);
      if (lookahead == '|') ADVANCE(304);
      if (lookahead != 0) ADVANCE(303);
      END_STATE();
    case 7:
      if (lookahead == '#') ADVANCE(193);
      if (lookahead == '.') ADVANCE(246);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(37);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(313);
      END_STATE();
    case 8:
      if (lookahead == '#') ADVANCE(9);
      if (lookahead == '.') ADVANCE(15);
      if (lookahead == '/') ADVANCE(260);
      if (lookahead == 'i') ADVANCE(310);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(8);
      END_STATE();
    case 9:
      if (lookahead == '#') ADVANCE(9);
      if (lookahead == '.') ADVANCE(14);
      if (lookahead == '/') ADVANCE(260);
      if (lookahead == 'i') ADVANCE(310);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(177);
      END_STATE();
    case 10:
      ADVANCE_MAP(
        '#', 9,
        '.', 12,
        '/', 260,
        'i', 310,
        '|', 255,
        'E', 184,
        'e', 184,
        'D', 184,
        'F', 184,
        'L', 184,
        'S', 184,
        'd', 184,
        'f', 184,
        'l', 184,
        's', 184,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(10);
      END_STATE();
    case 11:
      if (lookahead == '#') ADVANCE(9);
      if (lookahead == '.') ADVANCE(13);
      if (lookahead == '/') ADVANCE(260);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(255);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(184);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(11);
      END_STATE();
    case 12:
      ADVANCE_MAP(
        '#', 14,
        'i', 310,
        '|', 255,
        'E', 184,
        'e', 184,
        'D', 184,
        'F', 184,
        'L', 184,
        'S', 184,
        'd', 184,
        'f', 184,
        'l', 184,
        's', 184,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(12);
      END_STATE();
    case 13:
      if (lookahead == '#') ADVANCE(14);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(255);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(184);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(13);
      END_STATE();
    case 14:
      if (lookahead == '#') ADVANCE(14);
      if (lookahead == 'i') ADVANCE(310);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(177);
      END_STATE();
    case 15:
      if (lookahead == '#') ADVANCE(14);
      if (lookahead == 'i') ADVANCE(310);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(15);
      END_STATE();
    case 16:
      if (lookahead == '#') ADVANCE(16);
      if (lookahead == '/') ADVANCE(228);
      if (lookahead == 'i') ADVANCE(310);
      END_STATE();
    case 17:
      if (lookahead == '#') ADVANCE(16);
      if (lookahead == '/') ADVANCE(228);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(17);
      END_STATE();
    case 18:
      if (lookahead == '#') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(310);
      END_STATE();
    case 19:
      if (lookahead == '#') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(19);
      END_STATE();
    case 20:
      if (lookahead == '#') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(20);
      END_STATE();
    case 21:
      if (lookahead == '#') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(21);
      END_STATE();
    case 22:
      if (lookahead == '#') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(22);
      END_STATE();
    case 23:
      if (lookahead == '#') ADVANCE(4);
      if (lookahead == ';') ADVANCE(297);
      if (lookahead == '|') ADVANCE(118);
      if (set_contains(aux_sym__intertoken_token1_character_set_1, 9, lookahead)) ADVANCE(296);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '#' &&
          (lookahead < '\'' || ')' < lookahead) &&
          lookahead != ',' &&
          (lookahead < '[' || ']' < lookahead) &&
          lookahead != '`' &&
          (lookahead < '{' || '}' < lookahead)) ADVANCE(301);
      END_STATE();
    case 24:
      if (lookahead == '#') ADVANCE(236);
      if (lookahead == '.') ADVANCE(246);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(37);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(313);
      END_STATE();
    case 25:
      if (lookahead == '#') ADVANCE(25);
      if (lookahead == '/') ADVANCE(240);
      if (lookahead == 'i') ADVANCE(310);
      END_STATE();
    case 26:
      if (lookahead == '#') ADVANCE(25);
      if (lookahead == '/') ADVANCE(240);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(26);
      END_STATE();
    case 27:
      if (lookahead == '#') ADVANCE(27);
      if (lookahead == '/') ADVANCE(287);
      if (lookahead == 'i') ADVANCE(310);
      END_STATE();
    case 28:
      if (lookahead == '#') ADVANCE(27);
      if (lookahead == '/') ADVANCE(287);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(28);
      END_STATE();
    case 29:
      if (lookahead == '(') ADVANCE(620);
      END_STATE();
    case 30:
      if (lookahead == '.') ADVANCE(140);
      if (lookahead == '/') ADVANCE(255);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(255);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(184);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(30);
      END_STATE();
    case 31:
      if (lookahead == '.') ADVANCE(67);
      END_STATE();
    case 32:
      if (lookahead == '.') ADVANCE(142);
      if (lookahead == '/') ADVANCE(255);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(32);
      END_STATE();
    case 33:
      ADVANCE_MAP(
        '.', 139,
        '/', 255,
        'i', 310,
        '|', 255,
        'E', 184,
        'e', 184,
        'D', 184,
        'F', 184,
        'L', 184,
        'S', 184,
        'd', 184,
        'f', 184,
        'l', 184,
        's', 184,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(33);
      END_STATE();
    case 34:
      if (lookahead == '.') ADVANCE(246);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(37);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(313);
      END_STATE();
    case 35:
      if (lookahead == '.') ADVANCE(66);
      END_STATE();
    case 36:
      if (lookahead == '.') ADVANCE(68);
      END_STATE();
    case 37:
      if (lookahead == '.') ADVANCE(250);
      if (lookahead == 'I') ADVANCE(216);
      if (lookahead == 'N') ADVANCE(190);
      if (lookahead == 'i') ADVANCE(440);
      if (lookahead == 'n') ADVANCE(85);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(320);
      END_STATE();
    case 38:
      if (lookahead == '.') ADVANCE(253);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(39);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(331);
      END_STATE();
    case 39:
      if (lookahead == '.') ADVANCE(253);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(331);
      END_STATE();
    case 40:
      if (lookahead == '.') ADVANCE(254);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(8);
      END_STATE();
    case 41:
      if (lookahead == '.') ADVANCE(257);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(333);
      END_STATE();
    case 42:
      if (lookahead == '.') ADVANCE(257);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(41);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(333);
      END_STATE();
    case 43:
      if (lookahead == '.') ADVANCE(258);
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(10);
      END_STATE();
    case 44:
      if (lookahead == '.') ADVANCE(261);
      if (lookahead == 'i') ADVANCE(162);
      if (lookahead == 'n') ADVANCE(122);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(388);
      END_STATE();
    case 45:
      if (lookahead == '.') ADVANCE(261);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(44);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(388);
      END_STATE();
    case 46:
      if (lookahead == '.') ADVANCE(262);
      if (lookahead == 'i') ADVANCE(447);
      if (lookahead == 'n') ADVANCE(121);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(30);
      END_STATE();
    case 47:
      if (lookahead == '.') ADVANCE(265);
      if (lookahead == 'i') ADVANCE(162);
      if (lookahead == 'n') ADVANCE(122);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(335);
      END_STATE();
    case 48:
      if (lookahead == '.') ADVANCE(265);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(47);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(335);
      END_STATE();
    case 49:
      if (lookahead == '.') ADVANCE(266);
      if (lookahead == 'i') ADVANCE(447);
      if (lookahead == 'n') ADVANCE(121);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(11);
      END_STATE();
    case 50:
      if (lookahead == '.') ADVANCE(267);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(51);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(391);
      END_STATE();
    case 51:
      if (lookahead == '.') ADVANCE(267);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(211);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(188);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(391);
      END_STATE();
    case 52:
      if (lookahead == '.') ADVANCE(268);
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'i') ADVANCE(474);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(189);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(32);
      END_STATE();
    case 53:
      if (lookahead == '.') ADVANCE(269);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(392);
      END_STATE();
    case 54:
      if (lookahead == '.') ADVANCE(269);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(53);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(392);
      END_STATE();
    case 55:
      if (lookahead == '.') ADVANCE(270);
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(33);
      END_STATE();
    case 56:
      if (lookahead == '.') ADVANCE(69);
      END_STATE();
    case 57:
      if (lookahead == '.') ADVANCE(70);
      END_STATE();
    case 58:
      if (lookahead == '.') ADVANCE(71);
      END_STATE();
    case 59:
      if (lookahead == '.') ADVANCE(72);
      END_STATE();
    case 60:
      if (lookahead == '.') ADVANCE(73);
      END_STATE();
    case 61:
      if (lookahead == '.') ADVANCE(74);
      END_STATE();
    case 62:
      if (lookahead == '.') ADVANCE(75);
      END_STATE();
    case 63:
      if (lookahead == '/') ADVANCE(229);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(63);
      END_STATE();
    case 64:
      if (lookahead == '/') ADVANCE(241);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(64);
      END_STATE();
    case 65:
      if (lookahead == '/') ADVANCE(289);
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(65);
      END_STATE();
    case 66:
      if (lookahead == '0') ADVANCE(310);
      END_STATE();
    case 67:
      if (lookahead == '0') ADVANCE(414);
      END_STATE();
    case 68:
      if (lookahead == '0') ADVANCE(138);
      END_STATE();
    case 69:
      if (lookahead == '0') ADVANCE(418);
      END_STATE();
    case 70:
      if (lookahead == '0') ADVANCE(428);
      END_STATE();
    case 71:
      if (lookahead == '0') ADVANCE(429);
      END_STATE();
    case 72:
      if (lookahead == '0') ADVANCE(415);
      END_STATE();
    case 73:
      if (lookahead == '0') ADVANCE(419);
      END_STATE();
    case 74:
      if (lookahead == '0') ADVANCE(416);
      END_STATE();
    case 75:
      if (lookahead == '0') ADVANCE(420);
      END_STATE();
    case 76:
      if (lookahead == '8') ADVANCE(29);
      END_STATE();
    case 77:
      if (lookahead == ';') ADVANCE(116);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(77);
      END_STATE();
    case 78:
      if (lookahead == ';') ADVANCE(503);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(78);
      END_STATE();
    case 79:
      if (lookahead == ';') ADVANCE(117);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(79);
      END_STATE();
    case 80:
      if (lookahead == ';') ADVANCE(118);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(80);
      END_STATE();
    case 81:
      if (lookahead == 'A') ADVANCE(194);
      if (lookahead == 'a') ADVANCE(88);
      END_STATE();
    case 82:
      if (lookahead == 'A') ADVANCE(207);
      if (lookahead == 'a') ADVANCE(107);
      END_STATE();
    case 83:
      if (lookahead == 'A') ADVANCE(209);
      if (lookahead == 'a') ADVANCE(109);
      END_STATE();
    case 84:
      if (lookahead == 'A') ADVANCE(210);
      if (lookahead == 'a') ADVANCE(110);
      END_STATE();
    case 85:
      if (lookahead == 'A') ADVANCE(213);
      if (lookahead == 'a') ADVANCE(112);
      END_STATE();
    case 86:
      if (lookahead == 'A') ADVANCE(214);
      if (lookahead == 'a') ADVANCE(113);
      END_STATE();
    case 87:
      if (lookahead == 'A') ADVANCE(215);
      if (lookahead == 'a') ADVANCE(114);
      END_STATE();
    case 88:
      if (lookahead == 'C') ADVANCE(195);
      if (lookahead == 'c') ADVANCE(195);
      END_STATE();
    case 89:
      if (lookahead == 'F') ADVANCE(31);
      if (lookahead == 'f') ADVANCE(56);
      END_STATE();
    case 90:
      if (lookahead == 'F') ADVANCE(35);
      if (lookahead == 'f') ADVANCE(35);
      END_STATE();
    case 91:
      if (lookahead == 'F') ADVANCE(36);
      if (lookahead == 'f') ADVANCE(36);
      END_STATE();
    case 92:
      if (lookahead == 'F') ADVANCE(57);
      if (lookahead == 'f') ADVANCE(58);
      END_STATE();
    case 93:
      if (lookahead == 'F') ADVANCE(59);
      if (lookahead == 'f') ADVANCE(60);
      END_STATE();
    case 94:
      if (lookahead == 'F') ADVANCE(61);
      if (lookahead == 'f') ADVANCE(62);
      END_STATE();
    case 95:
      if (lookahead == 'I') ADVANCE(208);
      if (lookahead == 'i') ADVANCE(108);
      END_STATE();
    case 96:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (lookahead == '|') ADVANCE(255);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(17);
      END_STATE();
    case 97:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (lookahead == '|') ADVANCE(255);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(63);
      END_STATE();
    case 98:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(26);
      END_STATE();
    case 99:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(64);
      END_STATE();
    case 100:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(28);
      END_STATE();
    case 101:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'N') ADVANCE(189);
      if (lookahead == 'i') ADVANCE(439);
      if (lookahead == 'n') ADVANCE(84);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(65);
      END_STATE();
    case 102:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'i') ADVANCE(474);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(189);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(63);
      END_STATE();
    case 103:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'i') ADVANCE(474);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(189);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(64);
      END_STATE();
    case 104:
      if (lookahead == 'I') ADVANCE(212);
      if (lookahead == 'i') ADVANCE(474);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(189);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(65);
      END_STATE();
    case 105:
      if (lookahead == 'L') ADVANCE(203);
      if (lookahead == 'l') ADVANCE(95);
      END_STATE();
    case 106:
      ADVANCE_MAP(
        'N', 498,
        'S', 499,
        'X', 500,
        'a', 495,
        'b', 487,
        'd', 492,
        'e', 496,
        'l', 493,
        'n', 485,
        'p', 488,
        'r', 491,
        's', 486,
        't', 490,
        'u', 500,
        'v', 497,
        'x', 500,
      );
      if (lookahead != 0) ADVANCE(484);
      END_STATE();
    case 107:
      if (lookahead == 'N') ADVANCE(31);
      if (lookahead == 'n') ADVANCE(56);
      END_STATE();
    case 108:
      if (lookahead == 'N') ADVANCE(195);
      if (lookahead == 'n') ADVANCE(195);
      END_STATE();
    case 109:
      if (lookahead == 'N') ADVANCE(35);
      if (lookahead == 'n') ADVANCE(35);
      END_STATE();
    case 110:
      if (lookahead == 'N') ADVANCE(36);
      if (lookahead == 'n') ADVANCE(36);
      END_STATE();
    case 111:
      if (lookahead == 'N') ADVANCE(198);
      if (lookahead == 'n') ADVANCE(90);
      END_STATE();
    case 112:
      if (lookahead == 'N') ADVANCE(57);
      if (lookahead == 'n') ADVANCE(58);
      END_STATE();
    case 113:
      if (lookahead == 'N') ADVANCE(59);
      if (lookahead == 'n') ADVANCE(60);
      END_STATE();
    case 114:
      if (lookahead == 'N') ADVANCE(61);
      if (lookahead == 'n') ADVANCE(62);
      END_STATE();
    case 115:
      if (lookahead == 'W') ADVANCE(205);
      if (lookahead == 'l') ADVANCE(484);
      if (lookahead == 'w') ADVANCE(105);
      END_STATE();
    case 116:
      if (lookahead == '\\') ADVANCE(222);
      if (lookahead == '|') ADVANCE(508);
      if (lookahead != 0) ADVANCE(116);
      END_STATE();
    case 117:
      if (lookahead == '\\') ADVANCE(223);
      if (lookahead == '|') ADVANCE(602);
      if (lookahead != 0) ADVANCE(117);
      END_STATE();
    case 118:
      if (lookahead == '\\') ADVANCE(224);
      if (lookahead == '|') ADVANCE(300);
      if (lookahead != 0) ADVANCE(118);
      END_STATE();
    case 119:
      if (lookahead == 'a') ADVANCE(166);
      END_STATE();
    case 120:
      if (lookahead == 'a') ADVANCE(127);
      END_STATE();
    case 121:
      if (lookahead == 'a') ADVANCE(161);
      END_STATE();
    case 122:
      if (lookahead == 'a') ADVANCE(160);
      END_STATE();
    case 123:
      if (lookahead == 'b') ADVANCE(484);
      END_STATE();
    case 124:
      if (lookahead == 'b') ADVANCE(163);
      END_STATE();
    case 125:
      if (lookahead == 'c') ADVANCE(153);
      END_STATE();
    case 126:
      if (lookahead == 'c') ADVANCE(489);
      END_STATE();
    case 127:
      if (lookahead == 'c') ADVANCE(129);
      END_STATE();
    case 128:
      if (lookahead == 'd') ADVANCE(484);
      END_STATE();
    case 129:
      if (lookahead == 'e') ADVANCE(484);
      END_STATE();
    case 130:
      if (lookahead == 'e') ADVANCE(134);
      END_STATE();
    case 131:
      if (lookahead == 'e') ADVANCE(128);
      END_STATE();
    case 132:
      if (lookahead == 'e') ADVANCE(171);
      END_STATE();
    case 133:
      if (lookahead == 'e') ADVANCE(131);
      END_STATE();
    case 134:
      if (lookahead == 'f') ADVANCE(133);
      END_STATE();
    case 135:
      if (lookahead == 'f') ADVANCE(35);
      END_STATE();
    case 136:
      if (lookahead == 'f') ADVANCE(36);
      END_STATE();
    case 137:
      if (lookahead == 'g') ADVANCE(129);
      END_STATE();
    case 138:
      if (lookahead == 'i') ADVANCE(310);
      END_STATE();
    case 139:
      ADVANCE_MAP(
        'i', 310,
        '|', 255,
        'E', 184,
        'e', 184,
        'D', 184,
        'F', 184,
        'L', 184,
        'S', 184,
        'd', 184,
        'f', 184,
        'l', 184,
        's', 184,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(139);
      END_STATE();
    case 140:
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(255);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(184);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(140);
      END_STATE();
    case 141:
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(141);
      END_STATE();
    case 142:
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(177);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(142);
      END_STATE();
    case 143:
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(17);
      END_STATE();
    case 144:
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(144);
      END_STATE();
    case 145:
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(26);
      END_STATE();
    case 146:
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(146);
      END_STATE();
    case 147:
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(147);
      END_STATE();
    case 148:
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(28);
      END_STATE();
    case 149:
      if (lookahead == 'i') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(149);
      END_STATE();
    case 150:
      if (lookahead == 'i') ADVANCE(447);
      if (lookahead == 'n') ADVANCE(121);
      if (lookahead == '|') ADVANCE(255);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(63);
      END_STATE();
    case 151:
      if (lookahead == 'i') ADVANCE(447);
      if (lookahead == 'n') ADVANCE(121);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(64);
      END_STATE();
    case 152:
      if (lookahead == 'i') ADVANCE(447);
      if (lookahead == 'n') ADVANCE(121);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(65);
      END_STATE();
    case 153:
      if (lookahead == 'k') ADVANCE(168);
      END_STATE();
    case 154:
      if (lookahead == 'l') ADVANCE(484);
      END_STATE();
    case 155:
      if (lookahead == 'l') ADVANCE(132);
      END_STATE();
    case 156:
      if (lookahead == 'l') ADVANCE(494);
      END_STATE();
    case 157:
      if (lookahead == 'm') ADVANCE(484);
      END_STATE();
    case 158:
      if (lookahead == 'n') ADVANCE(484);
      END_STATE();
    case 159:
      if (lookahead == 'n') ADVANCE(130);
      END_STATE();
    case 160:
      if (lookahead == 'n') ADVANCE(35);
      END_STATE();
    case 161:
      if (lookahead == 'n') ADVANCE(36);
      END_STATE();
    case 162:
      if (lookahead == 'n') ADVANCE(135);
      END_STATE();
    case 163:
      if (lookahead == 'o') ADVANCE(174);
      END_STATE();
    case 164:
      if (lookahead == 'p') ADVANCE(120);
      END_STATE();
    case 165:
      if (lookahead == 'p') ADVANCE(129);
      END_STATE();
    case 166:
      if (lookahead == 'r') ADVANCE(157);
      END_STATE();
    case 167:
      if (lookahead == 'r') ADVANCE(158);
      END_STATE();
    case 168:
      if (lookahead == 's') ADVANCE(164);
      END_STATE();
    case 169:
      if (lookahead == 't') ADVANCE(484);
      END_STATE();
    case 170:
      if (lookahead == 't') ADVANCE(173);
      END_STATE();
    case 171:
      if (lookahead == 't') ADVANCE(129);
      END_STATE();
    case 172:
      if (lookahead == 'u') ADVANCE(76);
      END_STATE();
    case 173:
      if (lookahead == 'u') ADVANCE(167);
      END_STATE();
    case 174:
      if (lookahead == 'u') ADVANCE(169);
      END_STATE();
    case 175:
      if (lookahead == '|') ADVANCE(117);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(603);
      END_STATE();
    case 176:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(479);
      END_STATE();
    case 177:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(147);
      END_STATE();
    case 178:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(251);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(422);
      END_STATE();
    case 179:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(274);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(427);
      END_STATE();
    case 180:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(282);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(404);
      END_STATE();
    case 181:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(256);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(421);
      END_STATE();
    case 182:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(276);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(426);
      END_STATE();
    case 183:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(263);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(461);
      END_STATE();
    case 184:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(264);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(141);
      END_STATE();
    case 185:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(281);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(405);
      END_STATE();
    case 186:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(194);
      END_STATE();
    case 187:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(207);
      END_STATE();
    case 188:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(209);
      END_STATE();
    case 189:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(210);
      END_STATE();
    case 190:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(213);
      END_STATE();
    case 191:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(214);
      END_STATE();
    case 192:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(215);
      END_STATE();
    case 193:
      ADVANCE_MAP(
        'B', 399,
        'b', 399,
        'D', 34,
        'd', 34,
        'O', 401,
        'o', 401,
        'X', 403,
        'x', 403,
      );
      END_STATE();
    case 194:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(195);
      END_STATE();
    case 195:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(484);
      END_STATE();
    case 196:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(307);
      END_STATE();
    case 197:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(31);
      END_STATE();
    case 198:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(35);
      END_STATE();
    case 199:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(36);
      END_STATE();
    case 200:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(57);
      END_STATE();
    case 201:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(59);
      END_STATE();
    case 202:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(61);
      END_STATE();
    case 203:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(208);
      END_STATE();
    case 204:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(219);
      END_STATE();
    case 205:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(203);
      END_STATE();
    case 206:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(197);
      END_STATE();
    case 207:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(31);
      END_STATE();
    case 208:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(195);
      END_STATE();
    case 209:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(35);
      END_STATE();
    case 210:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(36);
      END_STATE();
    case 211:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(198);
      END_STATE();
    case 212:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(199);
      END_STATE();
    case 213:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(57);
      END_STATE();
    case 214:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(59);
      END_STATE();
    case 215:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(61);
      END_STATE();
    case 216:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(200);
      END_STATE();
    case 217:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(201);
      END_STATE();
    case 218:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(202);
      END_STATE();
    case 219:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(196);
      END_STATE();
    case 220:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(196);
      END_STATE();
    case 221:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(205);
      END_STATE();
    case 222:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(283);
      if (lookahead == 'a' ||
          lookahead == 'b' ||
          lookahead == 'n' ||
          lookahead == 'r' ||
          lookahead == 't' ||
          lookahead == '|') ADVANCE(116);
      END_STATE();
    case 223:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(288);
      if (lookahead == 'a' ||
          lookahead == 'b' ||
          lookahead == 'n' ||
          lookahead == 'r' ||
          lookahead == 't' ||
          lookahead == '|') ADVANCE(117);
      END_STATE();
    case 224:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(293);
      if (lookahead == 'a' ||
          lookahead == 'b' ||
          lookahead == 'n' ||
          lookahead == 'r' ||
          lookahead == 't' ||
          lookahead == '|') ADVANCE(118);
      END_STATE();
    case 225:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(358);
      END_STATE();
    case 226:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(351);
      END_STATE();
    case 227:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(365);
      END_STATE();
    case 228:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(19);
      END_STATE();
    case 229:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(144);
      END_STATE();
    case 230:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(360);
      END_STATE();
    case 231:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(367);
      END_STATE();
    case 232:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(476);
      END_STATE();
    case 233:
      if (lookahead == 'E' ||
          lookahead == 'I' ||
          lookahead == 'e' ||
          lookahead == 'i') ADVANCE(399);
      END_STATE();
    case 234:
      if (lookahead == 'E' ||
          lookahead == 'I' ||
          lookahead == 'e' ||
          lookahead == 'i') ADVANCE(401);
      END_STATE();
    case 235:
      if (lookahead == 'E' ||
          lookahead == 'I' ||
          lookahead == 'e' ||
          lookahead == 'i') ADVANCE(403);
      END_STATE();
    case 236:
      if (lookahead == 'E' ||
          lookahead == 'I' ||
          lookahead == 'e' ||
          lookahead == 'i') ADVANCE(34);
      END_STATE();
    case 237:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(371);
      END_STATE();
    case 238:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(352);
      END_STATE();
    case 239:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(378);
      END_STATE();
    case 240:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(20);
      END_STATE();
    case 241:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(146);
      END_STATE();
    case 242:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(373);
      END_STATE();
    case 243:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(380);
      END_STATE();
    case 244:
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(478);
      END_STATE();
    case 245:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(425);
      END_STATE();
    case 246:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(317);
      END_STATE();
    case 247:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(424);
      END_STATE();
    case 248:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(328);
      END_STATE();
    case 249:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(479);
      END_STATE();
    case 250:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(325);
      END_STATE();
    case 251:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(422);
      END_STATE();
    case 252:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(342);
      END_STATE();
    case 253:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(349);
      END_STATE();
    case 254:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(15);
      END_STATE();
    case 255:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(147);
      END_STATE();
    case 256:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(421);
      END_STATE();
    case 257:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(345);
      END_STATE();
    case 258:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(12);
      END_STATE();
    case 259:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(354);
      END_STATE();
    case 260:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(21);
      END_STATE();
    case 261:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(459);
      END_STATE();
    case 262:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(140);
      END_STATE();
    case 263:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(461);
      END_STATE();
    case 264:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(141);
      END_STATE();
    case 265:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(347);
      END_STATE();
    case 266:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(13);
      END_STATE();
    case 267:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(469);
      END_STATE();
    case 268:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(142);
      END_STATE();
    case 269:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(458);
      END_STATE();
    case 270:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(139);
      END_STATE();
    case 271:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(409);
      END_STATE();
    case 272:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(408);
      END_STATE();
    case 273:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(329);
      END_STATE();
    case 274:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(427);
      END_STATE();
    case 275:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(343);
      END_STATE();
    case 276:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(426);
      END_STATE();
    case 277:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(411);
      END_STATE();
    case 278:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(410);
      END_STATE();
    case 279:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(413);
      END_STATE();
    case 280:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(412);
      END_STATE();
    case 281:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(405);
      END_STATE();
    case 282:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(404);
      END_STATE();
    case 283:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(77);
      END_STATE();
    case 284:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(382);
      END_STATE();
    case 285:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(355);
      END_STATE();
    case 286:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(385);
      END_STATE();
    case 287:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(22);
      END_STATE();
    case 288:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(79);
      END_STATE();
    case 289:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(149);
      END_STATE();
    case 290:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(384);
      END_STATE();
    case 291:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(387);
      END_STATE();
    case 292:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(482);
      END_STATE();
    case 293:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(80);
      END_STATE();
    case 294:
      if (eof) ADVANCE(295);
      ADVANCE_MAP(
        '"', 501,
        '#', 3,
        '\'', 610,
        '(', 604,
        ')', 605,
        ',', 615,
        '.', 579,
        ';', 297,
        '[', 606,
        ']', 607,
        '`', 611,
        '{', 608,
        '|', 116,
        '}', 609,
        '+', 517,
        '-', 517,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(311);
      if (set_contains(aux_sym__intertoken_token1_character_set_1, 9, lookahead)) ADVANCE(296);
      if (lookahead != 0 &&
          (lookahead < '[' || ']' < lookahead)) ADVANCE(601);
      END_STATE();
    case 295:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 296:
      ACCEPT_TOKEN(aux_sym__intertoken_token1);
      if (set_contains(aux_sym__intertoken_token1_character_set_1, 9, lookahead)) ADVANCE(296);
      END_STATE();
    case 297:
      ACCEPT_TOKEN(aux_sym_comment_token1);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(297);
      END_STATE();
    case 298:
      ACCEPT_TOKEN(anon_sym_POUND_SEMI);
      END_STATE();
    case 299:
      ACCEPT_TOKEN(anon_sym_POUND_BANG);
      END_STATE();
    case 300:
      ACCEPT_TOKEN(aux_sym_directive_token1);
      END_STATE();
    case 301:
      ACCEPT_TOKEN(aux_sym_directive_token1);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(301);
      END_STATE();
    case 302:
      ACCEPT_TOKEN(anon_sym_POUND_PIPE);
      END_STATE();
    case 303:
      ACCEPT_TOKEN(aux_sym_block_comment_token1);
      END_STATE();
    case 304:
      ACCEPT_TOKEN(aux_sym_block_comment_token1);
      if (lookahead == '#') ADVANCE(306);
      END_STATE();
    case 305:
      ACCEPT_TOKEN(aux_sym_block_comment_token1);
      if (lookahead == '|') ADVANCE(302);
      END_STATE();
    case 306:
      ACCEPT_TOKEN(anon_sym_PIPE_POUND);
      END_STATE();
    case 307:
      ACCEPT_TOKEN(sym_boolean);
      END_STATE();
    case 308:
      ACCEPT_TOKEN(sym_boolean);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(204);
      END_STATE();
    case 309:
      ACCEPT_TOKEN(sym_boolean);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(220);
      END_STATE();
    case 310:
      ACCEPT_TOKEN(sym_number);
      END_STATE();
    case 311:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 312,
        '.', 315,
        '/', 581,
        '@', 521,
        '|', 245,
        '+', 522,
        '-', 522,
        'E', 559,
        'e', 559,
        'D', 560,
        'F', 560,
        'L', 560,
        'S', 560,
        'd', 560,
        'f', 560,
        'l', 560,
        's', 560,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(311);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 312:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(312);
      if (lookahead == '.') ADVANCE(316);
      if (lookahead == '/') ADVANCE(248);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(185);
      END_STATE();
    case 313:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 312,
        '.', 317,
        '/', 273,
        '@', 42,
        '|', 245,
        '+', 43,
        '-', 43,
        'E', 178,
        'e', 178,
        'D', 179,
        'F', 179,
        'L', 179,
        'S', 179,
        'd', 179,
        'f', 179,
        'l', 179,
        's', 179,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(313);
      END_STATE();
    case 314:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(233);
      if (lookahead == '@') ADVANCE(448);
      if (lookahead == '|') ADVANCE(271);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(398);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(321);
      END_STATE();
    case 315:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 316,
        '@', 521,
        '|', 245,
        '+', 522,
        '-', 522,
        'E', 559,
        'e', 559,
        'D', 560,
        'F', 560,
        'L', 560,
        'S', 560,
        'd', 560,
        'f', 560,
        'l', 560,
        's', 560,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(315);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 316:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(316);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(185);
      END_STATE();
    case 317:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 316,
        '@', 42,
        '|', 245,
        '+', 43,
        '-', 43,
        'E', 178,
        'e', 178,
        'D', 179,
        'F', 179,
        'L', 179,
        'S', 179,
        'd', 179,
        'f', 179,
        'l', 179,
        's', 179,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(317);
      END_STATE();
    case 318:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 319,
        '.', 323,
        '/', 583,
        '@', 521,
        'i', 483,
        '|', 247,
        '+', 522,
        '-', 522,
        'E', 561,
        'e', 561,
        'D', 562,
        'F', 562,
        'L', 562,
        'S', 562,
        'd', 562,
        'f', 562,
        'l', 562,
        's', 562,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(318);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 319:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(319);
      if (lookahead == '.') ADVANCE(324);
      if (lookahead == '/') ADVANCE(252);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(180);
      END_STATE();
    case 320:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 319,
        '.', 325,
        '/', 275,
        '@', 42,
        'i', 310,
        '|', 247,
        '+', 43,
        '-', 43,
        'E', 181,
        'e', 181,
        'D', 182,
        'F', 182,
        'L', 182,
        'S', 182,
        'd', 182,
        'f', 182,
        'l', 182,
        's', 182,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(320);
      END_STATE();
    case 321:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(322);
      if (lookahead == '/') ADVANCE(225);
      if (lookahead == '@') ADVANCE(448);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(96);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(321);
      END_STATE();
    case 322:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(322);
      if (lookahead == '/') ADVANCE(230);
      if (lookahead == '@') ADVANCE(462);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(143);
      END_STATE();
    case 323:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 324,
        '@', 521,
        'i', 483,
        '|', 247,
        '+', 522,
        '-', 522,
        'E', 561,
        'e', 561,
        'D', 562,
        'F', 562,
        'L', 562,
        'S', 562,
        'd', 562,
        'f', 562,
        'l', 562,
        's', 562,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(323);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 324:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(324);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(180);
      END_STATE();
    case 325:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 324,
        '@', 42,
        'i', 310,
        '|', 247,
        '+', 43,
        '-', 43,
        'E', 181,
        'e', 181,
        'D', 182,
        'F', 182,
        'L', 182,
        'S', 182,
        'd', 182,
        'f', 182,
        'l', 182,
        's', 182,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(325);
      END_STATE();
    case 326:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(327);
      if (lookahead == '@') ADVANCE(521);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(522);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(326);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 327:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(327);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      END_STATE();
    case 328:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(327);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(328);
      END_STATE();
    case 329:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(327);
      if (lookahead == '@') ADVANCE(42);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(43);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(329);
      END_STATE();
    case 330:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 332,
        '.', 344,
        '/', 587,
        '|', 249,
        'E', 563,
        'e', 563,
        'D', 563,
        'F', 563,
        'L', 563,
        'S', 563,
        'd', 563,
        'f', 563,
        'l', 563,
        's', 563,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(330);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 331:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(332);
      if (lookahead == '.') ADVANCE(349);
      if (lookahead == '/') ADVANCE(259);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(176);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(331);
      END_STATE();
    case 332:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(332);
      if (lookahead == '.') ADVANCE(348);
      if (lookahead == '/') ADVANCE(259);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(176);
      END_STATE();
    case 333:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 332,
        '.', 345,
        '/', 259,
        '|', 249,
        'E', 183,
        'e', 183,
        'D', 183,
        'F', 183,
        'L', 183,
        'S', 183,
        'd', 183,
        'f', 183,
        'l', 183,
        's', 183,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(333);
      END_STATE();
    case 334:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(332);
      if (lookahead == '.') ADVANCE(346);
      if (lookahead == '/') ADVANCE(587);
      if (lookahead == '|') ADVANCE(249);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(563);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(334);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 335:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(332);
      if (lookahead == '.') ADVANCE(347);
      if (lookahead == '/') ADVANCE(259);
      if (lookahead == '|') ADVANCE(249);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(183);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(335);
      END_STATE();
    case 336:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(336);
      if (lookahead == '/') ADVANCE(226);
      END_STATE();
    case 337:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(336);
      if (lookahead == '/') ADVANCE(226);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(337);
      END_STATE();
    case 338:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(339);
      if (lookahead == '/') ADVANCE(227);
      if (lookahead == '@') ADVANCE(448);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(96);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(338);
      END_STATE();
    case 339:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(339);
      if (lookahead == '/') ADVANCE(231);
      if (lookahead == '@') ADVANCE(462);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(143);
      END_STATE();
    case 340:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(341);
      if (lookahead == '@') ADVANCE(521);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(522);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(340);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 341:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(341);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      END_STATE();
    case 342:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(341);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(342);
      END_STATE();
    case 343:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(341);
      if (lookahead == '@') ADVANCE(42);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(43);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(343);
      END_STATE();
    case 344:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 348,
        '|', 249,
        'E', 563,
        'e', 563,
        'D', 563,
        'F', 563,
        'L', 563,
        'S', 563,
        'd', 563,
        'f', 563,
        'l', 563,
        's', 563,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(344);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 345:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '#', 348,
        '|', 249,
        'E', 183,
        'e', 183,
        'D', 183,
        'F', 183,
        'L', 183,
        'S', 183,
        'd', 183,
        'f', 183,
        'l', 183,
        's', 183,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(345);
      END_STATE();
    case 346:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(348);
      if (lookahead == '|') ADVANCE(249);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(563);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(346);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 347:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(348);
      if (lookahead == '|') ADVANCE(249);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(183);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(347);
      END_STATE();
    case 348:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(348);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(176);
      END_STATE();
    case 349:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(348);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(176);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(349);
      END_STATE();
    case 350:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(350);
      END_STATE();
    case 351:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(350);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(351);
      END_STATE();
    case 352:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(350);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(352);
      END_STATE();
    case 353:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(350);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(353);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 354:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(350);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(354);
      END_STATE();
    case 355:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(350);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(355);
      END_STATE();
    case 356:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(357);
      if (lookahead == '/') ADVANCE(237);
      if (lookahead == '@') ADVANCE(449);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(98);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(356);
      END_STATE();
    case 357:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(357);
      if (lookahead == '/') ADVANCE(242);
      if (lookahead == '@') ADVANCE(463);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(145);
      END_STATE();
    case 358:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(359);
      if (lookahead == '@') ADVANCE(448);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(96);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(358);
      END_STATE();
    case 359:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(359);
      if (lookahead == '@') ADVANCE(462);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(143);
      END_STATE();
    case 360:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(359);
      if (lookahead == '@') ADVANCE(462);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(143);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(360);
      END_STATE();
    case 361:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(361);
      if (lookahead == '/') ADVANCE(238);
      END_STATE();
    case 362:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(361);
      if (lookahead == '/') ADVANCE(238);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(362);
      END_STATE();
    case 363:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(364);
      if (lookahead == '/') ADVANCE(239);
      if (lookahead == '@') ADVANCE(449);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(98);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(363);
      END_STATE();
    case 364:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(364);
      if (lookahead == '/') ADVANCE(243);
      if (lookahead == '@') ADVANCE(463);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(145);
      END_STATE();
    case 365:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(366);
      if (lookahead == '@') ADVANCE(448);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(96);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(365);
      END_STATE();
    case 366:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(366);
      if (lookahead == '@') ADVANCE(462);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(143);
      END_STATE();
    case 367:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(366);
      if (lookahead == '@') ADVANCE(462);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(143);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(367);
      END_STATE();
    case 368:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(234);
      if (lookahead == '@') ADVANCE(449);
      if (lookahead == '|') ADVANCE(277);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(400);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(356);
      END_STATE();
    case 369:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(370);
      if (lookahead == '/') ADVANCE(284);
      if (lookahead == '@') ADVANCE(450);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(100);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(369);
      END_STATE();
    case 370:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(370);
      if (lookahead == '/') ADVANCE(290);
      if (lookahead == '@') ADVANCE(464);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(148);
      END_STATE();
    case 371:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(372);
      if (lookahead == '@') ADVANCE(449);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(98);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(371);
      END_STATE();
    case 372:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(372);
      if (lookahead == '@') ADVANCE(463);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(145);
      END_STATE();
    case 373:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(372);
      if (lookahead == '@') ADVANCE(463);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(145);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(373);
      END_STATE();
    case 374:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(374);
      if (lookahead == '/') ADVANCE(285);
      END_STATE();
    case 375:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(374);
      if (lookahead == '/') ADVANCE(285);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(375);
      END_STATE();
    case 376:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(377);
      if (lookahead == '/') ADVANCE(286);
      if (lookahead == '@') ADVANCE(450);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(100);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(376);
      END_STATE();
    case 377:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(377);
      if (lookahead == '/') ADVANCE(291);
      if (lookahead == '@') ADVANCE(464);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(148);
      END_STATE();
    case 378:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(379);
      if (lookahead == '@') ADVANCE(449);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(98);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(378);
      END_STATE();
    case 379:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(379);
      if (lookahead == '@') ADVANCE(463);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(145);
      END_STATE();
    case 380:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(379);
      if (lookahead == '@') ADVANCE(463);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(145);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(380);
      END_STATE();
    case 381:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(235);
      if (lookahead == '@') ADVANCE(450);
      if (lookahead == '|') ADVANCE(279);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(402);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(369);
      END_STATE();
    case 382:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(383);
      if (lookahead == '@') ADVANCE(450);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(100);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(382);
      END_STATE();
    case 383:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(383);
      if (lookahead == '@') ADVANCE(464);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(148);
      END_STATE();
    case 384:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(383);
      if (lookahead == '@') ADVANCE(464);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(384);
      END_STATE();
    case 385:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(386);
      if (lookahead == '@') ADVANCE(450);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(100);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(385);
      END_STATE();
    case 386:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(386);
      if (lookahead == '@') ADVANCE(464);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(148);
      END_STATE();
    case 387:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '#') ADVANCE(386);
      if (lookahead == '@') ADVANCE(464);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(387);
      END_STATE();
    case 388:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '.') ADVANCE(459);
      if (lookahead == '/') ADVANCE(249);
      if (lookahead == '|') ADVANCE(249);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(183);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(388);
      END_STATE();
    case 389:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '.') ADVANCE(468);
      if (lookahead == '/') ADVANCE(597);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(565);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(389);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 390:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '.', 457,
        '/', 597,
        '|', 249,
        'E', 563,
        'e', 563,
        'D', 563,
        'F', 563,
        'L', 563,
        'S', 563,
        'd', 563,
        'f', 563,
        'l', 563,
        's', 563,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(390);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 391:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '.') ADVANCE(469);
      if (lookahead == '/') ADVANCE(249);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(176);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(391);
      END_STATE();
    case 392:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '.', 458,
        '/', 249,
        '|', 249,
        'E', 183,
        'e', 183,
        'D', 183,
        'F', 183,
        'L', 183,
        'S', 183,
        'd', 183,
        'f', 183,
        'l', 183,
        's', 183,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(392);
      END_STATE();
    case 393:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '/') ADVANCE(232);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 394:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '/') ADVANCE(244);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 395:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '/') ADVANCE(292);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 396:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(521);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '|') ADVANCE(247);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(522);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(396);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 397:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(521);
      if (lookahead == '|') ADVANCE(245);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(522);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(397);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 398:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '@', 448,
        'I', 206,
        'N', 187,
        'i', 437,
        'n', 82,
        '|', 272,
        '+', 96,
        '-', 96,
        '0', 338,
        '1', 338,
      );
      END_STATE();
    case 399:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(448);
      if (lookahead == '|') ADVANCE(271);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(398);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(321);
      END_STATE();
    case 400:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '@', 449,
        'I', 217,
        'N', 191,
        'i', 441,
        'n', 86,
        '|', 278,
        '+', 98,
        '-', 98,
      );
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(363);
      END_STATE();
    case 401:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(449);
      if (lookahead == '|') ADVANCE(277);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(400);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(356);
      END_STATE();
    case 402:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '@', 450,
        'I', 218,
        'N', 192,
        'i', 442,
        'n', 87,
        '|', 280,
        '+', 100,
        '-', 100,
      );
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(376);
      END_STATE();
    case 403:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(450);
      if (lookahead == '|') ADVANCE(279);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(402);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(369);
      END_STATE();
    case 404:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(404);
      END_STATE();
    case 405:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(38);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(40);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(405);
      END_STATE();
    case 406:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(524);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '|') ADVANCE(247);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(525);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(406);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 407:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(524);
      if (lookahead == '|') ADVANCE(245);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(525);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(407);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 408:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(451);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(150);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(408);
      END_STATE();
    case 409:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(451);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(150);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(409);
      END_STATE();
    case 410:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(452);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(151);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(410);
      END_STATE();
    case 411:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(452);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(151);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(411);
      END_STATE();
    case 412:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(453);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(152);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(412);
      END_STATE();
    case 413:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(453);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(152);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(413);
      END_STATE();
    case 414:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(465);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(102);
      END_STATE();
    case 415:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(466);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(103);
      END_STATE();
    case 416:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(467);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(104);
      END_STATE();
    case 417:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(526);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(528);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 418:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(454);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(97);
      END_STATE();
    case 419:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(455);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(99);
      END_STATE();
    case 420:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(456);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(101);
      END_STATE();
    case 421:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(42);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(247);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(43);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(421);
      END_STATE();
    case 422:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(42);
      if (lookahead == '|') ADVANCE(245);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(43);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(422);
      END_STATE();
    case 423:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(530);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(531);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 424:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(45);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(46);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(424);
      END_STATE();
    case 425:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(45);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(46);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(425);
      END_STATE();
    case 426:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(48);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '|') ADVANCE(247);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(49);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(426);
      END_STATE();
    case 427:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(48);
      if (lookahead == '|') ADVANCE(245);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(49);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(427);
      END_STATE();
    case 428:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(50);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(52);
      END_STATE();
    case 429:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '@') ADVANCE(54);
      if (lookahead == 'i') ADVANCE(310);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(55);
      END_STATE();
    case 430:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(337);
      END_STATE();
    case 431:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 432:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(362);
      END_STATE();
    case 433:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 434:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(375);
      END_STATE();
    case 435:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I') ADVANCE(211);
      if (lookahead == 'N') ADVANCE(188);
      if (lookahead == 'i') ADVANCE(111);
      if (lookahead == 'n') ADVANCE(83);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 436:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(570);
      if (lookahead == 'n') ADVANCE(540);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 437:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(197);
      if (lookahead == 'n') ADVANCE(89);
      END_STATE();
    case 438:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(571);
      if (lookahead == 'n') ADVANCE(541);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 439:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(199);
      if (lookahead == 'n') ADVANCE(91);
      END_STATE();
    case 440:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(200);
      if (lookahead == 'n') ADVANCE(92);
      END_STATE();
    case 441:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(201);
      if (lookahead == 'n') ADVANCE(93);
      END_STATE();
    case 442:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N') ADVANCE(202);
      if (lookahead == 'n') ADVANCE(94);
      END_STATE();
    case 443:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'i') ADVANCE(162);
      if (lookahead == 'n') ADVANCE(122);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 444:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'i') ADVANCE(162);
      if (lookahead == 'n') ADVANCE(122);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 445:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'i') ADVANCE(162);
      if (lookahead == 'n') ADVANCE(122);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 446:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'n') ADVANCE(549);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 447:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'n') ADVANCE(136);
      END_STATE();
    case 448:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(430);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(337);
      END_STATE();
    case 449:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(432);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(362);
      END_STATE();
    case 450:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(434);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(375);
      END_STATE();
    case 451:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(443);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 452:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(444);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 453:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(445);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 454:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(431);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 455:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(433);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 456:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(435);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 457:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '|', 249,
        'E', 563,
        'e', 563,
        'D', 563,
        'F', 563,
        'L', 563,
        'S', 563,
        'd', 563,
        'f', 563,
        'l', 563,
        's', 563,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(457);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 458:
      ACCEPT_TOKEN(sym_number);
      ADVANCE_MAP(
        '|', 249,
        'E', 183,
        'e', 183,
        'D', 183,
        'F', 183,
        'L', 183,
        'S', 183,
        'd', 183,
        'f', 183,
        'l', 183,
        's', 183,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(458);
      END_STATE();
    case 459:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(183);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(459);
      END_STATE();
    case 460:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(460);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 461:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '|') ADVANCE(249);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(461);
      END_STATE();
    case 462:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(475);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(337);
      END_STATE();
    case 463:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(477);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(362);
      END_STATE();
    case 464:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(481);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(375);
      END_STATE();
    case 465:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(470);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 466:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(471);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 467:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(472);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 468:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(565);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(468);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 469:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(176);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(469);
      END_STATE();
    case 470:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(211);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(188);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(393);
      END_STATE();
    case 471:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(211);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(188);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(394);
      END_STATE();
    case 472:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(211);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(188);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(395);
      END_STATE();
    case 473:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(571);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 474:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(199);
      END_STATE();
    case 475:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(337);
      END_STATE();
    case 476:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(476);
      END_STATE();
    case 477:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(362);
      END_STATE();
    case 478:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(478);
      END_STATE();
    case 479:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(479);
      END_STATE();
    case 480:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(480);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 481:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(375);
      END_STATE();
    case 482:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(482);
      END_STATE();
    case 483:
      ACCEPT_TOKEN(sym_number);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 484:
      ACCEPT_TOKEN(sym_character);
      END_STATE();
    case 485:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'E') ADVANCE(221);
      if (lookahead == 'e') ADVANCE(115);
      if (lookahead == 'u') ADVANCE(156);
      END_STATE();
    case 486:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'P') ADVANCE(186);
      if (lookahead == 'p') ADVANCE(81);
      END_STATE();
    case 487:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'a') ADVANCE(125);
      if (lookahead == 'e') ADVANCE(154);
      END_STATE();
    case 488:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'a') ADVANCE(137);
      END_STATE();
    case 489:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'a') ADVANCE(165);
      END_STATE();
    case 490:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'a') ADVANCE(123);
      END_STATE();
    case 491:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'e') ADVANCE(170);
      if (lookahead == 'u') ADVANCE(124);
      END_STATE();
    case 492:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'e') ADVANCE(155);
      END_STATE();
    case 493:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'i') ADVANCE(159);
      if (lookahead == 's') ADVANCE(484);
      END_STATE();
    case 494:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'l') ADVANCE(484);
      END_STATE();
    case 495:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'l') ADVANCE(119);
      END_STATE();
    case 496:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 's') ADVANCE(126);
      END_STATE();
    case 497:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 't') ADVANCE(490);
      END_STATE();
    case 498:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(221);
      END_STATE();
    case 499:
      ACCEPT_TOKEN(sym_character);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(186);
      END_STATE();
    case 500:
      ACCEPT_TOKEN(sym_character);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(500);
      END_STATE();
    case 501:
      ACCEPT_TOKEN(anon_sym_DQUOTE);
      END_STATE();
    case 502:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(502);
      END_STATE();
    case 503:
      ACCEPT_TOKEN(sym_escape_sequence);
      END_STATE();
    case 504:
      ACCEPT_TOKEN(sym_escape_sequence);
      if (lookahead == '\r') ADVANCE(505);
      if (lookahead == '\n' ||
          lookahead == 0x85 ||
          lookahead == 0x2028) ADVANCE(506);
      if (set_contains(sym_escape_sequence_character_set_2, 11, lookahead)) ADVANCE(2);
      END_STATE();
    case 505:
      ACCEPT_TOKEN(sym_escape_sequence);
      if (lookahead == '\n' ||
          lookahead == 0x85) ADVANCE(506);
      if (set_contains(sym_escape_sequence_character_set_1, 9, lookahead)) ADVANCE(506);
      END_STATE();
    case 506:
      ACCEPT_TOKEN(sym_escape_sequence);
      if ((set_contains(sym_escape_sequence_character_set_1, 9, lookahead)) &&
          lookahead != '\n' &&
          lookahead != 0x85) ADVANCE(506);
      END_STATE();
    case 507:
      ACCEPT_TOKEN(sym_escape_sequence);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(78);
      END_STATE();
    case 508:
      ACCEPT_TOKEN(sym_symbol);
      END_STATE();
    case 509:
      ACCEPT_TOKEN(sym_symbol);
      ADVANCE_MAP(
        '#', 9,
        '.', 511,
        '/', 589,
        'i', 483,
        '|', 255,
        'E', 564,
        'e', 564,
        'D', 564,
        'F', 564,
        'L', 564,
        'S', 564,
        'd', 564,
        'f', 564,
        'l', 564,
        's', 564,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(509);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 510:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '#') ADVANCE(9);
      if (lookahead == '.') ADVANCE(512);
      if (lookahead == '/') ADVANCE(589);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '|') ADVANCE(255);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(564);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(510);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 511:
      ACCEPT_TOKEN(sym_symbol);
      ADVANCE_MAP(
        '#', 14,
        'i', 483,
        '|', 255,
        'E', 564,
        'e', 564,
        'D', 564,
        'F', 564,
        'L', 564,
        'S', 564,
        'd', 564,
        'f', 564,
        'l', 564,
        's', 564,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(511);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 512:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '#') ADVANCE(14);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '|') ADVANCE(255);
      if (('D' <= lookahead && lookahead <= 'F') ||
          lookahead == 'L' ||
          lookahead == 'S' ||
          ('d' <= lookahead && lookahead <= 'f') ||
          lookahead == 'l' ||
          lookahead == 's') ADVANCE(564);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(512);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 513:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '#') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(483);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(513);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 514:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(534);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 515:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(553);
      if (lookahead == '/') ADVANCE(598);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(566);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(515);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 516:
      ACCEPT_TOKEN(sym_symbol);
      ADVANCE_MAP(
        '.', 551,
        '/', 598,
        'i', 483,
        '|', 255,
        'E', 564,
        'e', 564,
        'D', 564,
        'F', 564,
        'L', 564,
        'S', 564,
        'd', 564,
        'f', 564,
        'l', 564,
        's', 564,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(516);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 517:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(580);
      if (lookahead == 'I') ADVANCE(573);
      if (lookahead == 'N') ADVANCE(567);
      if (lookahead == 'i') ADVANCE(436);
      if (lookahead == 'n') ADVANCE(537);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(318);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 518:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(535);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 519:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(533);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 520:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(585);
      if (lookahead == 'I') ADVANCE(578);
      if (lookahead == 'N') ADVANCE(569);
      if (lookahead == 'i') ADVANCE(546);
      if (lookahead == 'n') ADVANCE(539);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(330);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 521:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(585);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(520);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(330);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 522:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(586);
      if (lookahead == 'I') ADVANCE(577);
      if (lookahead == 'N') ADVANCE(568);
      if (lookahead == 'i') ADVANCE(438);
      if (lookahead == 'n') ADVANCE(538);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(509);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 523:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(591);
      if (lookahead == 'i') ADVANCE(558);
      if (lookahead == 'n') ADVANCE(548);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(334);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 524:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(591);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(523);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(334);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 525:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(592);
      if (lookahead == 'i') ADVANCE(446);
      if (lookahead == 'n') ADVANCE(547);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(510);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 526:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(593);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(527);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(389);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 527:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(593);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(578);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(569);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(389);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 528:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(594);
      if (lookahead == 'I') ADVANCE(577);
      if (lookahead == 'i') ADVANCE(473);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(568);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(515);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 529:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(595);
      if (lookahead == 'I') ADVANCE(578);
      if (lookahead == 'N') ADVANCE(569);
      if (lookahead == 'i') ADVANCE(546);
      if (lookahead == 'n') ADVANCE(539);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(390);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 530:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(595);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(529);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(390);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 531:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(596);
      if (lookahead == 'I') ADVANCE(577);
      if (lookahead == 'N') ADVANCE(568);
      if (lookahead == 'i') ADVANCE(438);
      if (lookahead == 'n') ADVANCE(538);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(516);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 532:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '.') ADVANCE(536);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 533:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '0') ADVANCE(483);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 534:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '0') ADVANCE(417);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 535:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '0') ADVANCE(555);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 536:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '0') ADVANCE(423);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 537:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'A') ADVANCE(574);
      if (lookahead == 'a') ADVANCE(543);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 538:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'A') ADVANCE(575);
      if (lookahead == 'a') ADVANCE(544);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 539:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'A') ADVANCE(576);
      if (lookahead == 'a') ADVANCE(545);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 540:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'F') ADVANCE(514);
      if (lookahead == 'f') ADVANCE(532);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 541:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'F') ADVANCE(518);
      if (lookahead == 'f') ADVANCE(518);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 542:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'F') ADVANCE(519);
      if (lookahead == 'f') ADVANCE(519);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 543:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N') ADVANCE(514);
      if (lookahead == 'n') ADVANCE(532);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 544:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N') ADVANCE(518);
      if (lookahead == 'n') ADVANCE(518);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 545:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N') ADVANCE(519);
      if (lookahead == 'n') ADVANCE(519);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 546:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N') ADVANCE(572);
      if (lookahead == 'n') ADVANCE(542);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 547:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'a') ADVANCE(556);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 548:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'a') ADVANCE(557);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 549:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'f') ADVANCE(518);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 550:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'f') ADVANCE(519);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 551:
      ACCEPT_TOKEN(sym_symbol);
      ADVANCE_MAP(
        'i', 483,
        '|', 255,
        'E', 564,
        'e', 564,
        'D', 564,
        'F', 564,
        'L', 564,
        'S', 564,
        'd', 564,
        'f', 564,
        'l', 564,
        's', 564,
      );
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(551);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 552:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == '|') ADVANCE(255);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(552);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 553:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'i') ADVANCE(483);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(566);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(553);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 554:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'i') ADVANCE(483);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(554);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 555:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'i') ADVANCE(483);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 556:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'n') ADVANCE(518);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 557:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'n') ADVANCE(519);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 558:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'n') ADVANCE(550);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 559:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(582);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(397);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 560:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(599);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(407);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 561:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(584);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(396);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 562:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(600);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(406);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 563:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(588);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(460);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 564:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(590);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(552);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 565:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(597);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(480);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 566:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(598);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(554);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 567:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(574);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 568:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(575);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 569:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(576);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 570:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(514);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 571:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(518);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 572:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(519);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 573:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(570);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 574:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(514);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 575:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(518);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 576:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(519);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 577:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(571);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 578:
      ACCEPT_TOKEN(sym_symbol);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(572);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 579:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(315);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 580:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(323);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 581:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(326);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 582:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(397);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 583:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(340);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 584:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(396);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 585:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(344);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 586:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(511);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 587:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(353);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 588:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(460);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 589:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(513);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 590:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(552);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 591:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(346);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 592:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(512);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 593:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(468);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 594:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(553);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 595:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(457);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 596:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(551);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 597:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(480);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 598:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(554);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 599:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(407);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 600:
      ACCEPT_TOKEN(sym_symbol);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(406);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 601:
      ACCEPT_TOKEN(sym_symbol);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(601);
      END_STATE();
    case 602:
      ACCEPT_TOKEN(sym_keyword);
      END_STATE();
    case 603:
      ACCEPT_TOKEN(sym_keyword);
      if ((!eof && set_contains(aux_sym_directive_token1_character_set_2, 17, lookahead))) ADVANCE(603);
      END_STATE();
    case 604:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 605:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 606:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 607:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 608:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      END_STATE();
    case 609:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      END_STATE();
    case 610:
      ACCEPT_TOKEN(anon_sym_SQUOTE);
      END_STATE();
    case 611:
      ACCEPT_TOKEN(anon_sym_BQUOTE);
      END_STATE();
    case 612:
      ACCEPT_TOKEN(anon_sym_POUND_SQUOTE);
      END_STATE();
    case 613:
      ACCEPT_TOKEN(anon_sym_POUND_BQUOTE);
      END_STATE();
    case 614:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 615:
      ACCEPT_TOKEN(anon_sym_COMMA);
      if (lookahead == '@') ADVANCE(616);
      END_STATE();
    case 616:
      ACCEPT_TOKEN(anon_sym_COMMA_AT);
      END_STATE();
    case 617:
      ACCEPT_TOKEN(anon_sym_POUND_COMMA);
      if (lookahead == '@') ADVANCE(618);
      END_STATE();
    case 618:
      ACCEPT_TOKEN(anon_sym_POUND_COMMA_AT);
      END_STATE();
    case 619:
      ACCEPT_TOKEN(anon_sym_POUND_LPAREN);
      END_STATE();
    case 620:
      ACCEPT_TOKEN(anon_sym_POUNDvu8_LPAREN);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 294},
  [2] = {.lex_state = 294},
  [3] = {.lex_state = 294},
  [4] = {.lex_state = 294},
  [5] = {.lex_state = 294},
  [6] = {.lex_state = 294},
  [7] = {.lex_state = 294},
  [8] = {.lex_state = 294},
  [9] = {.lex_state = 294},
  [10] = {.lex_state = 294},
  [11] = {.lex_state = 294},
  [12] = {.lex_state = 294},
  [13] = {.lex_state = 294},
  [14] = {.lex_state = 294},
  [15] = {.lex_state = 294},
  [16] = {.lex_state = 294},
  [17] = {.lex_state = 294},
  [18] = {.lex_state = 294},
  [19] = {.lex_state = 294},
  [20] = {.lex_state = 294},
  [21] = {.lex_state = 294},
  [22] = {.lex_state = 294},
  [23] = {.lex_state = 294},
  [24] = {.lex_state = 294},
  [25] = {.lex_state = 294},
  [26] = {.lex_state = 294},
  [27] = {.lex_state = 294},
  [28] = {.lex_state = 294},
  [29] = {.lex_state = 294},
  [30] = {.lex_state = 294},
  [31] = {.lex_state = 294},
  [32] = {.lex_state = 294},
  [33] = {.lex_state = 294},
  [34] = {.lex_state = 294},
  [35] = {.lex_state = 294},
  [36] = {.lex_state = 294},
  [37] = {.lex_state = 294},
  [38] = {.lex_state = 294},
  [39] = {.lex_state = 294},
  [40] = {.lex_state = 294},
  [41] = {.lex_state = 294},
  [42] = {.lex_state = 294},
  [43] = {.lex_state = 294},
  [44] = {.lex_state = 294},
  [45] = {.lex_state = 294},
  [46] = {.lex_state = 294},
  [47] = {.lex_state = 294},
  [48] = {.lex_state = 294},
  [49] = {.lex_state = 294},
  [50] = {.lex_state = 294},
  [51] = {.lex_state = 294},
  [52] = {.lex_state = 294},
  [53] = {.lex_state = 294},
  [54] = {.lex_state = 294},
  [55] = {.lex_state = 294},
  [56] = {.lex_state = 294},
  [57] = {.lex_state = 294},
  [58] = {.lex_state = 294},
  [59] = {.lex_state = 294},
  [60] = {.lex_state = 294},
  [61] = {.lex_state = 294},
  [62] = {.lex_state = 294},
  [63] = {.lex_state = 294},
  [64] = {.lex_state = 294},
  [65] = {.lex_state = 294},
  [66] = {.lex_state = 294},
  [67] = {.lex_state = 294},
  [68] = {.lex_state = 294},
  [69] = {.lex_state = 294},
  [70] = {.lex_state = 294},
  [71] = {.lex_state = 294},
  [72] = {.lex_state = 294},
  [73] = {.lex_state = 294},
  [74] = {.lex_state = 294},
  [75] = {.lex_state = 294},
  [76] = {.lex_state = 294},
  [77] = {.lex_state = 294},
  [78] = {.lex_state = 294},
  [79] = {.lex_state = 294},
  [80] = {.lex_state = 294},
  [81] = {.lex_state = 294},
  [82] = {.lex_state = 294},
  [83] = {.lex_state = 294},
  [84] = {.lex_state = 294},
  [85] = {.lex_state = 294},
  [86] = {.lex_state = 294},
  [87] = {.lex_state = 294},
  [88] = {.lex_state = 294},
  [89] = {.lex_state = 294},
  [90] = {.lex_state = 294},
  [91] = {.lex_state = 294},
  [92] = {.lex_state = 23},
  [93] = {.lex_state = 23},
  [94] = {.lex_state = 23},
  [95] = {.lex_state = 23},
  [96] = {.lex_state = 23},
  [97] = {.lex_state = 23},
  [98] = {.lex_state = 23},
  [99] = {.lex_state = 23},
  [100] = {.lex_state = 23},
  [101] = {.lex_state = 23},
  [102] = {.lex_state = 23},
  [103] = {.lex_state = 23},
  [104] = {.lex_state = 23},
  [105] = {.lex_state = 23},
  [106] = {.lex_state = 23},
  [107] = {.lex_state = 23},
  [108] = {.lex_state = 23},
  [109] = {.lex_state = 23},
  [110] = {.lex_state = 23},
  [111] = {.lex_state = 23},
  [112] = {.lex_state = 23},
  [113] = {.lex_state = 23},
  [114] = {.lex_state = 23},
  [115] = {.lex_state = 23},
  [116] = {.lex_state = 23},
  [117] = {.lex_state = 23},
  [118] = {.lex_state = 23},
  [119] = {.lex_state = 23},
  [120] = {.lex_state = 23},
  [121] = {.lex_state = 23},
  [122] = {.lex_state = 23},
  [123] = {.lex_state = 23},
  [124] = {.lex_state = 23},
  [125] = {.lex_state = 23},
  [126] = {.lex_state = 23},
  [127] = {.lex_state = 23},
  [128] = {.lex_state = 6},
  [129] = {.lex_state = 6},
  [130] = {.lex_state = 6},
  [131] = {.lex_state = 6},
  [132] = {.lex_state = 6},
  [133] = {.lex_state = 6},
  [134] = {.lex_state = 6},
  [135] = {.lex_state = 5},
  [136] = {.lex_state = 5},
  [137] = {.lex_state = 5},
  [138] = {.lex_state = 5},
  [139] = {.lex_state = 5},
  [140] = {.lex_state = 6},
  [141] = {.lex_state = 6},
  [142] = {.lex_state = 0},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [aux_sym__intertoken_token1] = ACTIONS(1),
    [aux_sym_comment_token1] = ACTIONS(1),
    [anon_sym_POUND_PIPE] = ACTIONS(1),
    [aux_sym_block_comment_token1] = ACTIONS(1),
    [anon_sym_PIPE_POUND] = ACTIONS(1),
    [anon_sym_DQUOTE] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE] = ACTIONS(1),
    [anon_sym_SQUOTE] = ACTIONS(1),
    [anon_sym_BQUOTE] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
  },
  [1] = {
    [sym_program] = STATE(142),
    [sym__token] = STATE(7),
    [sym__intertoken] = STATE(7),
    [sym_comment] = STATE(7),
    [sym_directive] = STATE(7),
    [sym_block_comment] = STATE(7),
    [sym__datum] = STATE(7),
    [sym_string] = STATE(7),
    [sym_list] = STATE(7),
    [sym_quote] = STATE(7),
    [sym_quasiquote] = STATE(7),
    [sym_syntax] = STATE(7),
    [sym_quasisyntax] = STATE(7),
    [sym_unquote] = STATE(7),
    [sym_unquote_splicing] = STATE(7),
    [sym_unsyntax] = STATE(7),
    [sym_unsyntax_splicing] = STATE(7),
    [sym_vector] = STATE(7),
    [sym_byte_vector] = STATE(7),
    [aux_sym_program_repeat1] = STATE(7),
    [ts_builtin_sym_end] = ACTIONS(3),
    [aux_sym__intertoken_token1] = ACTIONS(5),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(5),
    [sym_number] = ACTIONS(15),
    [sym_character] = ACTIONS(5),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(15),
    [sym_keyword] = ACTIONS(5),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [2] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [ts_builtin_sym_end] = ACTIONS(45),
    [aux_sym__intertoken_token1] = ACTIONS(47),
    [aux_sym_comment_token1] = ACTIONS(50),
    [anon_sym_POUND_SEMI] = ACTIONS(53),
    [anon_sym_POUND_BANG] = ACTIONS(56),
    [anon_sym_POUND_PIPE] = ACTIONS(59),
    [sym_boolean] = ACTIONS(47),
    [sym_number] = ACTIONS(62),
    [sym_character] = ACTIONS(47),
    [anon_sym_DQUOTE] = ACTIONS(65),
    [sym_symbol] = ACTIONS(62),
    [sym_keyword] = ACTIONS(47),
    [anon_sym_LPAREN] = ACTIONS(68),
    [anon_sym_RPAREN] = ACTIONS(45),
    [anon_sym_LBRACK] = ACTIONS(71),
    [anon_sym_RBRACK] = ACTIONS(45),
    [anon_sym_LBRACE] = ACTIONS(74),
    [anon_sym_RBRACE] = ACTIONS(45),
    [anon_sym_SQUOTE] = ACTIONS(77),
    [anon_sym_BQUOTE] = ACTIONS(80),
    [anon_sym_POUND_SQUOTE] = ACTIONS(83),
    [anon_sym_POUND_BQUOTE] = ACTIONS(86),
    [anon_sym_COMMA] = ACTIONS(89),
    [anon_sym_COMMA_AT] = ACTIONS(92),
    [anon_sym_POUND_COMMA] = ACTIONS(95),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(98),
    [anon_sym_POUND_LPAREN] = ACTIONS(101),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(104),
  },
  [3] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(111),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [4] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(113),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [5] = {
    [sym__token] = STATE(3),
    [sym__intertoken] = STATE(3),
    [sym_comment] = STATE(3),
    [sym_directive] = STATE(3),
    [sym_block_comment] = STATE(3),
    [sym__datum] = STATE(3),
    [sym_string] = STATE(3),
    [sym_list] = STATE(3),
    [sym_quote] = STATE(3),
    [sym_quasiquote] = STATE(3),
    [sym_syntax] = STATE(3),
    [sym_quasisyntax] = STATE(3),
    [sym_unquote] = STATE(3),
    [sym_unquote_splicing] = STATE(3),
    [sym_unsyntax] = STATE(3),
    [sym_unsyntax_splicing] = STATE(3),
    [sym_vector] = STATE(3),
    [sym_byte_vector] = STATE(3),
    [aux_sym_program_repeat1] = STATE(3),
    [aux_sym__intertoken_token1] = ACTIONS(115),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(115),
    [sym_number] = ACTIONS(117),
    [sym_character] = ACTIONS(115),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(117),
    [sym_keyword] = ACTIONS(115),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(119),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [6] = {
    [sym__token] = STATE(12),
    [sym__intertoken] = STATE(12),
    [sym_comment] = STATE(12),
    [sym_directive] = STATE(12),
    [sym_block_comment] = STATE(12),
    [sym__datum] = STATE(12),
    [sym_string] = STATE(12),
    [sym_list] = STATE(12),
    [sym_quote] = STATE(12),
    [sym_quasiquote] = STATE(12),
    [sym_syntax] = STATE(12),
    [sym_quasisyntax] = STATE(12),
    [sym_unquote] = STATE(12),
    [sym_unquote_splicing] = STATE(12),
    [sym_unsyntax] = STATE(12),
    [sym_unsyntax_splicing] = STATE(12),
    [sym_vector] = STATE(12),
    [sym_byte_vector] = STATE(12),
    [aux_sym_program_repeat1] = STATE(12),
    [aux_sym__intertoken_token1] = ACTIONS(121),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(121),
    [sym_number] = ACTIONS(123),
    [sym_character] = ACTIONS(121),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(123),
    [sym_keyword] = ACTIONS(121),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(125),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [7] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [ts_builtin_sym_end] = ACTIONS(127),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [8] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(129),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [9] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_RBRACK] = ACTIONS(129),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [10] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_RBRACE] = ACTIONS(129),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [11] = {
    [sym__token] = STATE(8),
    [sym__intertoken] = STATE(8),
    [sym_comment] = STATE(8),
    [sym_directive] = STATE(8),
    [sym_block_comment] = STATE(8),
    [sym__datum] = STATE(8),
    [sym_string] = STATE(8),
    [sym_list] = STATE(8),
    [sym_quote] = STATE(8),
    [sym_quasiquote] = STATE(8),
    [sym_syntax] = STATE(8),
    [sym_quasisyntax] = STATE(8),
    [sym_unquote] = STATE(8),
    [sym_unquote_splicing] = STATE(8),
    [sym_unsyntax] = STATE(8),
    [sym_unsyntax_splicing] = STATE(8),
    [sym_vector] = STATE(8),
    [sym_byte_vector] = STATE(8),
    [aux_sym_program_repeat1] = STATE(8),
    [aux_sym__intertoken_token1] = ACTIONS(131),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(131),
    [sym_number] = ACTIONS(133),
    [sym_character] = ACTIONS(131),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(133),
    [sym_keyword] = ACTIONS(131),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(135),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [12] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(137),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [13] = {
    [sym__token] = STATE(9),
    [sym__intertoken] = STATE(9),
    [sym_comment] = STATE(9),
    [sym_directive] = STATE(9),
    [sym_block_comment] = STATE(9),
    [sym__datum] = STATE(9),
    [sym_string] = STATE(9),
    [sym_list] = STATE(9),
    [sym_quote] = STATE(9),
    [sym_quasiquote] = STATE(9),
    [sym_syntax] = STATE(9),
    [sym_quasisyntax] = STATE(9),
    [sym_unquote] = STATE(9),
    [sym_unquote_splicing] = STATE(9),
    [sym_unsyntax] = STATE(9),
    [sym_unsyntax_splicing] = STATE(9),
    [sym_vector] = STATE(9),
    [sym_byte_vector] = STATE(9),
    [aux_sym_program_repeat1] = STATE(9),
    [aux_sym__intertoken_token1] = ACTIONS(139),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(139),
    [sym_number] = ACTIONS(141),
    [sym_character] = ACTIONS(139),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(141),
    [sym_keyword] = ACTIONS(139),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_RBRACK] = ACTIONS(135),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [14] = {
    [sym__token] = STATE(19),
    [sym__intertoken] = STATE(19),
    [sym_comment] = STATE(19),
    [sym_directive] = STATE(19),
    [sym_block_comment] = STATE(19),
    [sym__datum] = STATE(19),
    [sym_string] = STATE(19),
    [sym_list] = STATE(19),
    [sym_quote] = STATE(19),
    [sym_quasiquote] = STATE(19),
    [sym_syntax] = STATE(19),
    [sym_quasisyntax] = STATE(19),
    [sym_unquote] = STATE(19),
    [sym_unquote_splicing] = STATE(19),
    [sym_unsyntax] = STATE(19),
    [sym_unsyntax_splicing] = STATE(19),
    [sym_vector] = STATE(19),
    [sym_byte_vector] = STATE(19),
    [aux_sym_program_repeat1] = STATE(19),
    [aux_sym__intertoken_token1] = ACTIONS(143),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(143),
    [sym_number] = ACTIONS(145),
    [sym_character] = ACTIONS(143),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(145),
    [sym_keyword] = ACTIONS(143),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(147),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [15] = {
    [sym__token] = STATE(20),
    [sym__intertoken] = STATE(20),
    [sym_comment] = STATE(20),
    [sym_directive] = STATE(20),
    [sym_block_comment] = STATE(20),
    [sym__datum] = STATE(20),
    [sym_string] = STATE(20),
    [sym_list] = STATE(20),
    [sym_quote] = STATE(20),
    [sym_quasiquote] = STATE(20),
    [sym_syntax] = STATE(20),
    [sym_quasisyntax] = STATE(20),
    [sym_unquote] = STATE(20),
    [sym_unquote_splicing] = STATE(20),
    [sym_unsyntax] = STATE(20),
    [sym_unsyntax_splicing] = STATE(20),
    [sym_vector] = STATE(20),
    [sym_byte_vector] = STATE(20),
    [aux_sym_program_repeat1] = STATE(20),
    [aux_sym__intertoken_token1] = ACTIONS(149),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(149),
    [sym_number] = ACTIONS(151),
    [sym_character] = ACTIONS(149),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(151),
    [sym_keyword] = ACTIONS(149),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_RBRACK] = ACTIONS(147),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [16] = {
    [sym__token] = STATE(21),
    [sym__intertoken] = STATE(21),
    [sym_comment] = STATE(21),
    [sym_directive] = STATE(21),
    [sym_block_comment] = STATE(21),
    [sym__datum] = STATE(21),
    [sym_string] = STATE(21),
    [sym_list] = STATE(21),
    [sym_quote] = STATE(21),
    [sym_quasiquote] = STATE(21),
    [sym_syntax] = STATE(21),
    [sym_quasisyntax] = STATE(21),
    [sym_unquote] = STATE(21),
    [sym_unquote_splicing] = STATE(21),
    [sym_unsyntax] = STATE(21),
    [sym_unsyntax_splicing] = STATE(21),
    [sym_vector] = STATE(21),
    [sym_byte_vector] = STATE(21),
    [aux_sym_program_repeat1] = STATE(21),
    [aux_sym__intertoken_token1] = ACTIONS(153),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(153),
    [sym_number] = ACTIONS(155),
    [sym_character] = ACTIONS(153),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(155),
    [sym_keyword] = ACTIONS(153),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_RBRACE] = ACTIONS(147),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [17] = {
    [sym__token] = STATE(22),
    [sym__intertoken] = STATE(22),
    [sym_comment] = STATE(22),
    [sym_directive] = STATE(22),
    [sym_block_comment] = STATE(22),
    [sym__datum] = STATE(22),
    [sym_string] = STATE(22),
    [sym_list] = STATE(22),
    [sym_quote] = STATE(22),
    [sym_quasiquote] = STATE(22),
    [sym_syntax] = STATE(22),
    [sym_quasisyntax] = STATE(22),
    [sym_unquote] = STATE(22),
    [sym_unquote_splicing] = STATE(22),
    [sym_unsyntax] = STATE(22),
    [sym_unsyntax_splicing] = STATE(22),
    [sym_vector] = STATE(22),
    [sym_byte_vector] = STATE(22),
    [aux_sym_program_repeat1] = STATE(22),
    [aux_sym__intertoken_token1] = ACTIONS(157),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(157),
    [sym_number] = ACTIONS(159),
    [sym_character] = ACTIONS(157),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(159),
    [sym_keyword] = ACTIONS(157),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(161),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [18] = {
    [sym__token] = STATE(4),
    [sym__intertoken] = STATE(4),
    [sym_comment] = STATE(4),
    [sym_directive] = STATE(4),
    [sym_block_comment] = STATE(4),
    [sym__datum] = STATE(4),
    [sym_string] = STATE(4),
    [sym_list] = STATE(4),
    [sym_quote] = STATE(4),
    [sym_quasiquote] = STATE(4),
    [sym_syntax] = STATE(4),
    [sym_quasisyntax] = STATE(4),
    [sym_unquote] = STATE(4),
    [sym_unquote_splicing] = STATE(4),
    [sym_unsyntax] = STATE(4),
    [sym_unsyntax_splicing] = STATE(4),
    [sym_vector] = STATE(4),
    [sym_byte_vector] = STATE(4),
    [aux_sym_program_repeat1] = STATE(4),
    [aux_sym__intertoken_token1] = ACTIONS(163),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(163),
    [sym_number] = ACTIONS(165),
    [sym_character] = ACTIONS(163),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(165),
    [sym_keyword] = ACTIONS(163),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(167),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [19] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(169),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [20] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_RBRACK] = ACTIONS(169),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [21] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_RBRACE] = ACTIONS(169),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [22] = {
    [sym__token] = STATE(2),
    [sym__intertoken] = STATE(2),
    [sym_comment] = STATE(2),
    [sym_directive] = STATE(2),
    [sym_block_comment] = STATE(2),
    [sym__datum] = STATE(2),
    [sym_string] = STATE(2),
    [sym_list] = STATE(2),
    [sym_quote] = STATE(2),
    [sym_quasiquote] = STATE(2),
    [sym_syntax] = STATE(2),
    [sym_quasisyntax] = STATE(2),
    [sym_unquote] = STATE(2),
    [sym_unquote_splicing] = STATE(2),
    [sym_unsyntax] = STATE(2),
    [sym_unsyntax_splicing] = STATE(2),
    [sym_vector] = STATE(2),
    [sym_byte_vector] = STATE(2),
    [aux_sym_program_repeat1] = STATE(2),
    [aux_sym__intertoken_token1] = ACTIONS(107),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(107),
    [sym_number] = ACTIONS(109),
    [sym_character] = ACTIONS(107),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(109),
    [sym_keyword] = ACTIONS(107),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_RPAREN] = ACTIONS(171),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [23] = {
    [sym__token] = STATE(10),
    [sym__intertoken] = STATE(10),
    [sym_comment] = STATE(10),
    [sym_directive] = STATE(10),
    [sym_block_comment] = STATE(10),
    [sym__datum] = STATE(10),
    [sym_string] = STATE(10),
    [sym_list] = STATE(10),
    [sym_quote] = STATE(10),
    [sym_quasiquote] = STATE(10),
    [sym_syntax] = STATE(10),
    [sym_quasisyntax] = STATE(10),
    [sym_unquote] = STATE(10),
    [sym_unquote_splicing] = STATE(10),
    [sym_unsyntax] = STATE(10),
    [sym_unsyntax_splicing] = STATE(10),
    [sym_vector] = STATE(10),
    [sym_byte_vector] = STATE(10),
    [aux_sym_program_repeat1] = STATE(10),
    [aux_sym__intertoken_token1] = ACTIONS(173),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(173),
    [sym_number] = ACTIONS(175),
    [sym_character] = ACTIONS(173),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(175),
    [sym_keyword] = ACTIONS(173),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_RBRACE] = ACTIONS(135),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [24] = {
    [sym__intertoken] = STATE(47),
    [sym_comment] = STATE(47),
    [sym_directive] = STATE(47),
    [sym_block_comment] = STATE(47),
    [sym__datum] = STATE(101),
    [sym_string] = STATE(101),
    [sym_list] = STATE(101),
    [sym_quote] = STATE(101),
    [sym_quasiquote] = STATE(101),
    [sym_syntax] = STATE(101),
    [sym_quasisyntax] = STATE(101),
    [sym_unquote] = STATE(101),
    [sym_unquote_splicing] = STATE(101),
    [sym_unsyntax] = STATE(101),
    [sym_unsyntax_splicing] = STATE(101),
    [sym_vector] = STATE(101),
    [sym_byte_vector] = STATE(101),
    [aux_sym_comment_repeat1] = STATE(47),
    [aux_sym__intertoken_token1] = ACTIONS(177),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(179),
    [sym_number] = ACTIONS(181),
    [sym_character] = ACTIONS(179),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(181),
    [sym_keyword] = ACTIONS(179),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [25] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(67),
    [sym_string] = STATE(67),
    [sym_list] = STATE(67),
    [sym_quote] = STATE(67),
    [sym_quasiquote] = STATE(67),
    [sym_syntax] = STATE(67),
    [sym_quasisyntax] = STATE(67),
    [sym_unquote] = STATE(67),
    [sym_unquote_splicing] = STATE(67),
    [sym_unsyntax] = STATE(67),
    [sym_unsyntax_splicing] = STATE(67),
    [sym_vector] = STATE(67),
    [sym_byte_vector] = STATE(67),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(213),
    [sym_number] = ACTIONS(215),
    [sym_character] = ACTIONS(213),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(215),
    [sym_keyword] = ACTIONS(213),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [26] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(68),
    [sym_string] = STATE(68),
    [sym_list] = STATE(68),
    [sym_quote] = STATE(68),
    [sym_quasiquote] = STATE(68),
    [sym_syntax] = STATE(68),
    [sym_quasisyntax] = STATE(68),
    [sym_unquote] = STATE(68),
    [sym_unquote_splicing] = STATE(68),
    [sym_unsyntax] = STATE(68),
    [sym_unsyntax_splicing] = STATE(68),
    [sym_vector] = STATE(68),
    [sym_byte_vector] = STATE(68),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(217),
    [sym_number] = ACTIONS(219),
    [sym_character] = ACTIONS(217),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(219),
    [sym_keyword] = ACTIONS(217),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [27] = {
    [sym__intertoken] = STATE(26),
    [sym_comment] = STATE(26),
    [sym_directive] = STATE(26),
    [sym_block_comment] = STATE(26),
    [sym__datum] = STATE(81),
    [sym_string] = STATE(81),
    [sym_list] = STATE(81),
    [sym_quote] = STATE(81),
    [sym_quasiquote] = STATE(81),
    [sym_syntax] = STATE(81),
    [sym_quasisyntax] = STATE(81),
    [sym_unquote] = STATE(81),
    [sym_unquote_splicing] = STATE(81),
    [sym_unsyntax] = STATE(81),
    [sym_unsyntax_splicing] = STATE(81),
    [sym_vector] = STATE(81),
    [sym_byte_vector] = STATE(81),
    [aux_sym_comment_repeat1] = STATE(26),
    [aux_sym__intertoken_token1] = ACTIONS(221),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(223),
    [sym_number] = ACTIONS(225),
    [sym_character] = ACTIONS(223),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(225),
    [sym_keyword] = ACTIONS(223),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [28] = {
    [sym__intertoken] = STATE(55),
    [sym_comment] = STATE(55),
    [sym_directive] = STATE(55),
    [sym_block_comment] = STATE(55),
    [sym__datum] = STATE(72),
    [sym_string] = STATE(72),
    [sym_list] = STATE(72),
    [sym_quote] = STATE(72),
    [sym_quasiquote] = STATE(72),
    [sym_syntax] = STATE(72),
    [sym_quasisyntax] = STATE(72),
    [sym_unquote] = STATE(72),
    [sym_unquote_splicing] = STATE(72),
    [sym_unsyntax] = STATE(72),
    [sym_unsyntax_splicing] = STATE(72),
    [sym_vector] = STATE(72),
    [sym_byte_vector] = STATE(72),
    [aux_sym_comment_repeat1] = STATE(55),
    [aux_sym__intertoken_token1] = ACTIONS(227),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(229),
    [sym_number] = ACTIONS(231),
    [sym_character] = ACTIONS(229),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(231),
    [sym_keyword] = ACTIONS(229),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [29] = {
    [sym__intertoken] = STATE(56),
    [sym_comment] = STATE(56),
    [sym_directive] = STATE(56),
    [sym_block_comment] = STATE(56),
    [sym__datum] = STATE(74),
    [sym_string] = STATE(74),
    [sym_list] = STATE(74),
    [sym_quote] = STATE(74),
    [sym_quasiquote] = STATE(74),
    [sym_syntax] = STATE(74),
    [sym_quasisyntax] = STATE(74),
    [sym_unquote] = STATE(74),
    [sym_unquote_splicing] = STATE(74),
    [sym_unsyntax] = STATE(74),
    [sym_unsyntax_splicing] = STATE(74),
    [sym_vector] = STATE(74),
    [sym_byte_vector] = STATE(74),
    [aux_sym_comment_repeat1] = STATE(56),
    [aux_sym__intertoken_token1] = ACTIONS(233),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(235),
    [sym_number] = ACTIONS(237),
    [sym_character] = ACTIONS(235),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(237),
    [sym_keyword] = ACTIONS(235),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [30] = {
    [sym__intertoken] = STATE(50),
    [sym_comment] = STATE(50),
    [sym_directive] = STATE(50),
    [sym_block_comment] = STATE(50),
    [sym__datum] = STATE(73),
    [sym_string] = STATE(73),
    [sym_list] = STATE(73),
    [sym_quote] = STATE(73),
    [sym_quasiquote] = STATE(73),
    [sym_syntax] = STATE(73),
    [sym_quasisyntax] = STATE(73),
    [sym_unquote] = STATE(73),
    [sym_unquote_splicing] = STATE(73),
    [sym_unsyntax] = STATE(73),
    [sym_unsyntax_splicing] = STATE(73),
    [sym_vector] = STATE(73),
    [sym_byte_vector] = STATE(73),
    [aux_sym_comment_repeat1] = STATE(50),
    [aux_sym__intertoken_token1] = ACTIONS(239),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(241),
    [sym_number] = ACTIONS(243),
    [sym_character] = ACTIONS(241),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(243),
    [sym_keyword] = ACTIONS(241),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [31] = {
    [sym__intertoken] = STATE(40),
    [sym_comment] = STATE(40),
    [sym_directive] = STATE(40),
    [sym_block_comment] = STATE(40),
    [sym__datum] = STATE(124),
    [sym_string] = STATE(124),
    [sym_list] = STATE(124),
    [sym_quote] = STATE(124),
    [sym_quasiquote] = STATE(124),
    [sym_syntax] = STATE(124),
    [sym_quasisyntax] = STATE(124),
    [sym_unquote] = STATE(124),
    [sym_unquote_splicing] = STATE(124),
    [sym_unsyntax] = STATE(124),
    [sym_unsyntax_splicing] = STATE(124),
    [sym_vector] = STATE(124),
    [sym_byte_vector] = STATE(124),
    [aux_sym_comment_repeat1] = STATE(40),
    [aux_sym__intertoken_token1] = ACTIONS(245),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(247),
    [sym_number] = ACTIONS(249),
    [sym_character] = ACTIONS(247),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(249),
    [sym_keyword] = ACTIONS(247),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [32] = {
    [sym__intertoken] = STATE(41),
    [sym_comment] = STATE(41),
    [sym_directive] = STATE(41),
    [sym_block_comment] = STATE(41),
    [sym__datum] = STATE(126),
    [sym_string] = STATE(126),
    [sym_list] = STATE(126),
    [sym_quote] = STATE(126),
    [sym_quasiquote] = STATE(126),
    [sym_syntax] = STATE(126),
    [sym_quasisyntax] = STATE(126),
    [sym_unquote] = STATE(126),
    [sym_unquote_splicing] = STATE(126),
    [sym_unsyntax] = STATE(126),
    [sym_unsyntax_splicing] = STATE(126),
    [sym_vector] = STATE(126),
    [sym_byte_vector] = STATE(126),
    [aux_sym_comment_repeat1] = STATE(41),
    [aux_sym__intertoken_token1] = ACTIONS(251),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(253),
    [sym_number] = ACTIONS(255),
    [sym_character] = ACTIONS(253),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(255),
    [sym_keyword] = ACTIONS(253),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [33] = {
    [sym__intertoken] = STATE(42),
    [sym_comment] = STATE(42),
    [sym_directive] = STATE(42),
    [sym_block_comment] = STATE(42),
    [sym__datum] = STATE(97),
    [sym_string] = STATE(97),
    [sym_list] = STATE(97),
    [sym_quote] = STATE(97),
    [sym_quasiquote] = STATE(97),
    [sym_syntax] = STATE(97),
    [sym_quasisyntax] = STATE(97),
    [sym_unquote] = STATE(97),
    [sym_unquote_splicing] = STATE(97),
    [sym_unsyntax] = STATE(97),
    [sym_unsyntax_splicing] = STATE(97),
    [sym_vector] = STATE(97),
    [sym_byte_vector] = STATE(97),
    [aux_sym_comment_repeat1] = STATE(42),
    [aux_sym__intertoken_token1] = ACTIONS(257),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(259),
    [sym_number] = ACTIONS(261),
    [sym_character] = ACTIONS(259),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(261),
    [sym_keyword] = ACTIONS(259),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [34] = {
    [sym__intertoken] = STATE(43),
    [sym_comment] = STATE(43),
    [sym_directive] = STATE(43),
    [sym_block_comment] = STATE(43),
    [sym__datum] = STATE(100),
    [sym_string] = STATE(100),
    [sym_list] = STATE(100),
    [sym_quote] = STATE(100),
    [sym_quasiquote] = STATE(100),
    [sym_syntax] = STATE(100),
    [sym_quasisyntax] = STATE(100),
    [sym_unquote] = STATE(100),
    [sym_unquote_splicing] = STATE(100),
    [sym_unsyntax] = STATE(100),
    [sym_unsyntax_splicing] = STATE(100),
    [sym_vector] = STATE(100),
    [sym_byte_vector] = STATE(100),
    [aux_sym_comment_repeat1] = STATE(43),
    [aux_sym__intertoken_token1] = ACTIONS(263),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(265),
    [sym_number] = ACTIONS(267),
    [sym_character] = ACTIONS(265),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(267),
    [sym_keyword] = ACTIONS(265),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [35] = {
    [sym__intertoken] = STATE(44),
    [sym_comment] = STATE(44),
    [sym_directive] = STATE(44),
    [sym_block_comment] = STATE(44),
    [sym__datum] = STATE(120),
    [sym_string] = STATE(120),
    [sym_list] = STATE(120),
    [sym_quote] = STATE(120),
    [sym_quasiquote] = STATE(120),
    [sym_syntax] = STATE(120),
    [sym_quasisyntax] = STATE(120),
    [sym_unquote] = STATE(120),
    [sym_unquote_splicing] = STATE(120),
    [sym_unsyntax] = STATE(120),
    [sym_unsyntax_splicing] = STATE(120),
    [sym_vector] = STATE(120),
    [sym_byte_vector] = STATE(120),
    [aux_sym_comment_repeat1] = STATE(44),
    [aux_sym__intertoken_token1] = ACTIONS(269),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(271),
    [sym_number] = ACTIONS(273),
    [sym_character] = ACTIONS(271),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(273),
    [sym_keyword] = ACTIONS(271),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [36] = {
    [sym__intertoken] = STATE(45),
    [sym_comment] = STATE(45),
    [sym_directive] = STATE(45),
    [sym_block_comment] = STATE(45),
    [sym__datum] = STATE(125),
    [sym_string] = STATE(125),
    [sym_list] = STATE(125),
    [sym_quote] = STATE(125),
    [sym_quasiquote] = STATE(125),
    [sym_syntax] = STATE(125),
    [sym_quasisyntax] = STATE(125),
    [sym_unquote] = STATE(125),
    [sym_unquote_splicing] = STATE(125),
    [sym_unsyntax] = STATE(125),
    [sym_unsyntax_splicing] = STATE(125),
    [sym_vector] = STATE(125),
    [sym_byte_vector] = STATE(125),
    [aux_sym_comment_repeat1] = STATE(45),
    [aux_sym__intertoken_token1] = ACTIONS(275),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(277),
    [sym_number] = ACTIONS(279),
    [sym_character] = ACTIONS(277),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(279),
    [sym_keyword] = ACTIONS(277),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [37] = {
    [sym__intertoken] = STATE(46),
    [sym_comment] = STATE(46),
    [sym_directive] = STATE(46),
    [sym_block_comment] = STATE(46),
    [sym__datum] = STATE(99),
    [sym_string] = STATE(99),
    [sym_list] = STATE(99),
    [sym_quote] = STATE(99),
    [sym_quasiquote] = STATE(99),
    [sym_syntax] = STATE(99),
    [sym_quasisyntax] = STATE(99),
    [sym_unquote] = STATE(99),
    [sym_unquote_splicing] = STATE(99),
    [sym_unsyntax] = STATE(99),
    [sym_unsyntax_splicing] = STATE(99),
    [sym_vector] = STATE(99),
    [sym_byte_vector] = STATE(99),
    [aux_sym_comment_repeat1] = STATE(46),
    [aux_sym__intertoken_token1] = ACTIONS(281),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(283),
    [sym_number] = ACTIONS(285),
    [sym_character] = ACTIONS(283),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(285),
    [sym_keyword] = ACTIONS(283),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [38] = {
    [sym__intertoken] = STATE(54),
    [sym_comment] = STATE(54),
    [sym_directive] = STATE(54),
    [sym_block_comment] = STATE(54),
    [sym__datum] = STATE(71),
    [sym_string] = STATE(71),
    [sym_list] = STATE(71),
    [sym_quote] = STATE(71),
    [sym_quasiquote] = STATE(71),
    [sym_syntax] = STATE(71),
    [sym_quasisyntax] = STATE(71),
    [sym_unquote] = STATE(71),
    [sym_unquote_splicing] = STATE(71),
    [sym_unsyntax] = STATE(71),
    [sym_unsyntax_splicing] = STATE(71),
    [sym_vector] = STATE(71),
    [sym_byte_vector] = STATE(71),
    [aux_sym_comment_repeat1] = STATE(54),
    [aux_sym__intertoken_token1] = ACTIONS(287),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(289),
    [sym_number] = ACTIONS(291),
    [sym_character] = ACTIONS(289),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(291),
    [sym_keyword] = ACTIONS(289),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [39] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(61),
    [sym_string] = STATE(61),
    [sym_list] = STATE(61),
    [sym_quote] = STATE(61),
    [sym_quasiquote] = STATE(61),
    [sym_syntax] = STATE(61),
    [sym_quasisyntax] = STATE(61),
    [sym_unquote] = STATE(61),
    [sym_unquote_splicing] = STATE(61),
    [sym_unsyntax] = STATE(61),
    [sym_unsyntax_splicing] = STATE(61),
    [sym_vector] = STATE(61),
    [sym_byte_vector] = STATE(61),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(293),
    [sym_number] = ACTIONS(295),
    [sym_character] = ACTIONS(293),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(295),
    [sym_keyword] = ACTIONS(293),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [40] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(110),
    [sym_string] = STATE(110),
    [sym_list] = STATE(110),
    [sym_quote] = STATE(110),
    [sym_quasiquote] = STATE(110),
    [sym_syntax] = STATE(110),
    [sym_quasisyntax] = STATE(110),
    [sym_unquote] = STATE(110),
    [sym_unquote_splicing] = STATE(110),
    [sym_unsyntax] = STATE(110),
    [sym_unsyntax_splicing] = STATE(110),
    [sym_vector] = STATE(110),
    [sym_byte_vector] = STATE(110),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(297),
    [sym_number] = ACTIONS(299),
    [sym_character] = ACTIONS(297),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(299),
    [sym_keyword] = ACTIONS(297),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [41] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(111),
    [sym_string] = STATE(111),
    [sym_list] = STATE(111),
    [sym_quote] = STATE(111),
    [sym_quasiquote] = STATE(111),
    [sym_syntax] = STATE(111),
    [sym_quasisyntax] = STATE(111),
    [sym_unquote] = STATE(111),
    [sym_unquote_splicing] = STATE(111),
    [sym_unsyntax] = STATE(111),
    [sym_unsyntax_splicing] = STATE(111),
    [sym_vector] = STATE(111),
    [sym_byte_vector] = STATE(111),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(301),
    [sym_number] = ACTIONS(303),
    [sym_character] = ACTIONS(301),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(303),
    [sym_keyword] = ACTIONS(301),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [42] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(112),
    [sym_string] = STATE(112),
    [sym_list] = STATE(112),
    [sym_quote] = STATE(112),
    [sym_quasiquote] = STATE(112),
    [sym_syntax] = STATE(112),
    [sym_quasisyntax] = STATE(112),
    [sym_unquote] = STATE(112),
    [sym_unquote_splicing] = STATE(112),
    [sym_unsyntax] = STATE(112),
    [sym_unsyntax_splicing] = STATE(112),
    [sym_vector] = STATE(112),
    [sym_byte_vector] = STATE(112),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(305),
    [sym_number] = ACTIONS(307),
    [sym_character] = ACTIONS(305),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(307),
    [sym_keyword] = ACTIONS(305),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [43] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(113),
    [sym_string] = STATE(113),
    [sym_list] = STATE(113),
    [sym_quote] = STATE(113),
    [sym_quasiquote] = STATE(113),
    [sym_syntax] = STATE(113),
    [sym_quasisyntax] = STATE(113),
    [sym_unquote] = STATE(113),
    [sym_unquote_splicing] = STATE(113),
    [sym_unsyntax] = STATE(113),
    [sym_unsyntax_splicing] = STATE(113),
    [sym_vector] = STATE(113),
    [sym_byte_vector] = STATE(113),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(309),
    [sym_number] = ACTIONS(311),
    [sym_character] = ACTIONS(309),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(311),
    [sym_keyword] = ACTIONS(309),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [44] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(114),
    [sym_string] = STATE(114),
    [sym_list] = STATE(114),
    [sym_quote] = STATE(114),
    [sym_quasiquote] = STATE(114),
    [sym_syntax] = STATE(114),
    [sym_quasisyntax] = STATE(114),
    [sym_unquote] = STATE(114),
    [sym_unquote_splicing] = STATE(114),
    [sym_unsyntax] = STATE(114),
    [sym_unsyntax_splicing] = STATE(114),
    [sym_vector] = STATE(114),
    [sym_byte_vector] = STATE(114),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(313),
    [sym_number] = ACTIONS(315),
    [sym_character] = ACTIONS(313),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(315),
    [sym_keyword] = ACTIONS(313),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [45] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(115),
    [sym_string] = STATE(115),
    [sym_list] = STATE(115),
    [sym_quote] = STATE(115),
    [sym_quasiquote] = STATE(115),
    [sym_syntax] = STATE(115),
    [sym_quasisyntax] = STATE(115),
    [sym_unquote] = STATE(115),
    [sym_unquote_splicing] = STATE(115),
    [sym_unsyntax] = STATE(115),
    [sym_unsyntax_splicing] = STATE(115),
    [sym_vector] = STATE(115),
    [sym_byte_vector] = STATE(115),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(317),
    [sym_number] = ACTIONS(319),
    [sym_character] = ACTIONS(317),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(319),
    [sym_keyword] = ACTIONS(317),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [46] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(116),
    [sym_string] = STATE(116),
    [sym_list] = STATE(116),
    [sym_quote] = STATE(116),
    [sym_quasiquote] = STATE(116),
    [sym_syntax] = STATE(116),
    [sym_quasisyntax] = STATE(116),
    [sym_unquote] = STATE(116),
    [sym_unquote_splicing] = STATE(116),
    [sym_unsyntax] = STATE(116),
    [sym_unsyntax_splicing] = STATE(116),
    [sym_vector] = STATE(116),
    [sym_byte_vector] = STATE(116),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(321),
    [sym_number] = ACTIONS(323),
    [sym_character] = ACTIONS(321),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(323),
    [sym_keyword] = ACTIONS(321),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [47] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(117),
    [sym_string] = STATE(117),
    [sym_list] = STATE(117),
    [sym_quote] = STATE(117),
    [sym_quasiquote] = STATE(117),
    [sym_syntax] = STATE(117),
    [sym_quasisyntax] = STATE(117),
    [sym_unquote] = STATE(117),
    [sym_unquote_splicing] = STATE(117),
    [sym_unsyntax] = STATE(117),
    [sym_unsyntax_splicing] = STATE(117),
    [sym_vector] = STATE(117),
    [sym_byte_vector] = STATE(117),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(325),
    [sym_number] = ACTIONS(327),
    [sym_character] = ACTIONS(325),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(327),
    [sym_keyword] = ACTIONS(325),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [48] = {
    [sym__intertoken] = STATE(59),
    [sym_comment] = STATE(59),
    [sym_directive] = STATE(59),
    [sym_block_comment] = STATE(59),
    [sym__datum] = STATE(98),
    [sym_string] = STATE(98),
    [sym_list] = STATE(98),
    [sym_quote] = STATE(98),
    [sym_quasiquote] = STATE(98),
    [sym_syntax] = STATE(98),
    [sym_quasisyntax] = STATE(98),
    [sym_unquote] = STATE(98),
    [sym_unquote_splicing] = STATE(98),
    [sym_unsyntax] = STATE(98),
    [sym_unsyntax_splicing] = STATE(98),
    [sym_vector] = STATE(98),
    [sym_byte_vector] = STATE(98),
    [aux_sym_comment_repeat1] = STATE(59),
    [aux_sym__intertoken_token1] = ACTIONS(329),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(331),
    [sym_number] = ACTIONS(333),
    [sym_character] = ACTIONS(331),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(333),
    [sym_keyword] = ACTIONS(331),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [49] = {
    [sym__intertoken] = STATE(57),
    [sym_comment] = STATE(57),
    [sym_directive] = STATE(57),
    [sym_block_comment] = STATE(57),
    [sym__datum] = STATE(75),
    [sym_string] = STATE(75),
    [sym_list] = STATE(75),
    [sym_quote] = STATE(75),
    [sym_quasiquote] = STATE(75),
    [sym_syntax] = STATE(75),
    [sym_quasisyntax] = STATE(75),
    [sym_unquote] = STATE(75),
    [sym_unquote_splicing] = STATE(75),
    [sym_unsyntax] = STATE(75),
    [sym_unsyntax_splicing] = STATE(75),
    [sym_vector] = STATE(75),
    [sym_byte_vector] = STATE(75),
    [aux_sym_comment_repeat1] = STATE(57),
    [aux_sym__intertoken_token1] = ACTIONS(335),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(337),
    [sym_number] = ACTIONS(339),
    [sym_character] = ACTIONS(337),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(339),
    [sym_keyword] = ACTIONS(337),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [50] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(86),
    [sym_string] = STATE(86),
    [sym_list] = STATE(86),
    [sym_quote] = STATE(86),
    [sym_quasiquote] = STATE(86),
    [sym_syntax] = STATE(86),
    [sym_quasisyntax] = STATE(86),
    [sym_unquote] = STATE(86),
    [sym_unquote_splicing] = STATE(86),
    [sym_unsyntax] = STATE(86),
    [sym_unsyntax_splicing] = STATE(86),
    [sym_vector] = STATE(86),
    [sym_byte_vector] = STATE(86),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(341),
    [sym_number] = ACTIONS(343),
    [sym_character] = ACTIONS(341),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(343),
    [sym_keyword] = ACTIONS(341),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [51] = {
    [sym__intertoken] = STATE(58),
    [sym_comment] = STATE(58),
    [sym_directive] = STATE(58),
    [sym_block_comment] = STATE(58),
    [sym__datum] = STATE(77),
    [sym_string] = STATE(77),
    [sym_list] = STATE(77),
    [sym_quote] = STATE(77),
    [sym_quasiquote] = STATE(77),
    [sym_syntax] = STATE(77),
    [sym_quasisyntax] = STATE(77),
    [sym_unquote] = STATE(77),
    [sym_unquote_splicing] = STATE(77),
    [sym_unsyntax] = STATE(77),
    [sym_unsyntax_splicing] = STATE(77),
    [sym_vector] = STATE(77),
    [sym_byte_vector] = STATE(77),
    [aux_sym_comment_repeat1] = STATE(58),
    [aux_sym__intertoken_token1] = ACTIONS(345),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(347),
    [sym_number] = ACTIONS(349),
    [sym_character] = ACTIONS(347),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(349),
    [sym_keyword] = ACTIONS(347),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [52] = {
    [sym__intertoken] = STATE(39),
    [sym_comment] = STATE(39),
    [sym_directive] = STATE(39),
    [sym_block_comment] = STATE(39),
    [sym__datum] = STATE(78),
    [sym_string] = STATE(78),
    [sym_list] = STATE(78),
    [sym_quote] = STATE(78),
    [sym_quasiquote] = STATE(78),
    [sym_syntax] = STATE(78),
    [sym_quasisyntax] = STATE(78),
    [sym_unquote] = STATE(78),
    [sym_unquote_splicing] = STATE(78),
    [sym_unsyntax] = STATE(78),
    [sym_unsyntax_splicing] = STATE(78),
    [sym_vector] = STATE(78),
    [sym_byte_vector] = STATE(78),
    [aux_sym_comment_repeat1] = STATE(39),
    [aux_sym__intertoken_token1] = ACTIONS(351),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(353),
    [sym_number] = ACTIONS(355),
    [sym_character] = ACTIONS(353),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(355),
    [sym_keyword] = ACTIONS(353),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [53] = {
    [sym__intertoken] = STATE(25),
    [sym_comment] = STATE(25),
    [sym_directive] = STATE(25),
    [sym_block_comment] = STATE(25),
    [sym__datum] = STATE(80),
    [sym_string] = STATE(80),
    [sym_list] = STATE(80),
    [sym_quote] = STATE(80),
    [sym_quasiquote] = STATE(80),
    [sym_syntax] = STATE(80),
    [sym_quasisyntax] = STATE(80),
    [sym_unquote] = STATE(80),
    [sym_unquote_splicing] = STATE(80),
    [sym_unsyntax] = STATE(80),
    [sym_unsyntax_splicing] = STATE(80),
    [sym_vector] = STATE(80),
    [sym_byte_vector] = STATE(80),
    [aux_sym_comment_repeat1] = STATE(25),
    [aux_sym__intertoken_token1] = ACTIONS(357),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(359),
    [sym_number] = ACTIONS(361),
    [sym_character] = ACTIONS(359),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(361),
    [sym_keyword] = ACTIONS(359),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [54] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(62),
    [sym_string] = STATE(62),
    [sym_list] = STATE(62),
    [sym_quote] = STATE(62),
    [sym_quasiquote] = STATE(62),
    [sym_syntax] = STATE(62),
    [sym_quasisyntax] = STATE(62),
    [sym_unquote] = STATE(62),
    [sym_unquote_splicing] = STATE(62),
    [sym_unsyntax] = STATE(62),
    [sym_unsyntax_splicing] = STATE(62),
    [sym_vector] = STATE(62),
    [sym_byte_vector] = STATE(62),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(363),
    [sym_number] = ACTIONS(365),
    [sym_character] = ACTIONS(363),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(365),
    [sym_keyword] = ACTIONS(363),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [55] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(63),
    [sym_string] = STATE(63),
    [sym_list] = STATE(63),
    [sym_quote] = STATE(63),
    [sym_quasiquote] = STATE(63),
    [sym_syntax] = STATE(63),
    [sym_quasisyntax] = STATE(63),
    [sym_unquote] = STATE(63),
    [sym_unquote_splicing] = STATE(63),
    [sym_unsyntax] = STATE(63),
    [sym_unsyntax_splicing] = STATE(63),
    [sym_vector] = STATE(63),
    [sym_byte_vector] = STATE(63),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(367),
    [sym_number] = ACTIONS(369),
    [sym_character] = ACTIONS(367),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(369),
    [sym_keyword] = ACTIONS(367),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [56] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(64),
    [sym_string] = STATE(64),
    [sym_list] = STATE(64),
    [sym_quote] = STATE(64),
    [sym_quasiquote] = STATE(64),
    [sym_syntax] = STATE(64),
    [sym_quasisyntax] = STATE(64),
    [sym_unquote] = STATE(64),
    [sym_unquote_splicing] = STATE(64),
    [sym_unsyntax] = STATE(64),
    [sym_unsyntax_splicing] = STATE(64),
    [sym_vector] = STATE(64),
    [sym_byte_vector] = STATE(64),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(371),
    [sym_number] = ACTIONS(373),
    [sym_character] = ACTIONS(371),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(373),
    [sym_keyword] = ACTIONS(371),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [57] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(91),
    [sym_string] = STATE(91),
    [sym_list] = STATE(91),
    [sym_quote] = STATE(91),
    [sym_quasiquote] = STATE(91),
    [sym_syntax] = STATE(91),
    [sym_quasisyntax] = STATE(91),
    [sym_unquote] = STATE(91),
    [sym_unquote_splicing] = STATE(91),
    [sym_unsyntax] = STATE(91),
    [sym_unsyntax_splicing] = STATE(91),
    [sym_vector] = STATE(91),
    [sym_byte_vector] = STATE(91),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(375),
    [sym_number] = ACTIONS(377),
    [sym_character] = ACTIONS(375),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(377),
    [sym_keyword] = ACTIONS(375),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [58] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(66),
    [sym_string] = STATE(66),
    [sym_list] = STATE(66),
    [sym_quote] = STATE(66),
    [sym_quasiquote] = STATE(66),
    [sym_syntax] = STATE(66),
    [sym_quasisyntax] = STATE(66),
    [sym_unquote] = STATE(66),
    [sym_unquote_splicing] = STATE(66),
    [sym_unsyntax] = STATE(66),
    [sym_unsyntax_splicing] = STATE(66),
    [sym_vector] = STATE(66),
    [sym_byte_vector] = STATE(66),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(379),
    [sym_number] = ACTIONS(381),
    [sym_character] = ACTIONS(379),
    [anon_sym_DQUOTE] = ACTIONS(17),
    [sym_symbol] = ACTIONS(381),
    [sym_keyword] = ACTIONS(379),
    [anon_sym_LPAREN] = ACTIONS(19),
    [anon_sym_LBRACK] = ACTIONS(21),
    [anon_sym_LBRACE] = ACTIONS(23),
    [anon_sym_SQUOTE] = ACTIONS(25),
    [anon_sym_BQUOTE] = ACTIONS(27),
    [anon_sym_POUND_SQUOTE] = ACTIONS(29),
    [anon_sym_POUND_BQUOTE] = ACTIONS(31),
    [anon_sym_COMMA] = ACTIONS(33),
    [anon_sym_COMMA_AT] = ACTIONS(35),
    [anon_sym_POUND_COMMA] = ACTIONS(37),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(39),
    [anon_sym_POUND_LPAREN] = ACTIONS(41),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(43),
  },
  [59] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [sym__datum] = STATE(105),
    [sym_string] = STATE(105),
    [sym_list] = STATE(105),
    [sym_quote] = STATE(105),
    [sym_quasiquote] = STATE(105),
    [sym_syntax] = STATE(105),
    [sym_quasisyntax] = STATE(105),
    [sym_unquote] = STATE(105),
    [sym_unquote_splicing] = STATE(105),
    [sym_unsyntax] = STATE(105),
    [sym_unsyntax_splicing] = STATE(105),
    [sym_vector] = STATE(105),
    [sym_byte_vector] = STATE(105),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(211),
    [aux_sym_comment_token1] = ACTIONS(7),
    [anon_sym_POUND_SEMI] = ACTIONS(9),
    [anon_sym_POUND_BANG] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [sym_boolean] = ACTIONS(383),
    [sym_number] = ACTIONS(385),
    [sym_character] = ACTIONS(383),
    [anon_sym_DQUOTE] = ACTIONS(183),
    [sym_symbol] = ACTIONS(385),
    [sym_keyword] = ACTIONS(383),
    [anon_sym_LPAREN] = ACTIONS(185),
    [anon_sym_LBRACK] = ACTIONS(187),
    [anon_sym_LBRACE] = ACTIONS(189),
    [anon_sym_SQUOTE] = ACTIONS(191),
    [anon_sym_BQUOTE] = ACTIONS(193),
    [anon_sym_POUND_SQUOTE] = ACTIONS(195),
    [anon_sym_POUND_BQUOTE] = ACTIONS(197),
    [anon_sym_COMMA] = ACTIONS(199),
    [anon_sym_COMMA_AT] = ACTIONS(201),
    [anon_sym_POUND_COMMA] = ACTIONS(203),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(205),
    [anon_sym_POUND_LPAREN] = ACTIONS(207),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(209),
  },
  [60] = {
    [sym__intertoken] = STATE(60),
    [sym_comment] = STATE(60),
    [sym_directive] = STATE(60),
    [sym_block_comment] = STATE(60),
    [aux_sym_comment_repeat1] = STATE(60),
    [aux_sym__intertoken_token1] = ACTIONS(387),
    [aux_sym_comment_token1] = ACTIONS(390),
    [anon_sym_POUND_SEMI] = ACTIONS(393),
    [anon_sym_POUND_BANG] = ACTIONS(396),
    [anon_sym_POUND_PIPE] = ACTIONS(399),
    [sym_boolean] = ACTIONS(402),
    [sym_number] = ACTIONS(404),
    [sym_character] = ACTIONS(402),
    [anon_sym_DQUOTE] = ACTIONS(402),
    [sym_symbol] = ACTIONS(404),
    [sym_keyword] = ACTIONS(402),
    [anon_sym_LPAREN] = ACTIONS(402),
    [anon_sym_LBRACK] = ACTIONS(402),
    [anon_sym_LBRACE] = ACTIONS(402),
    [anon_sym_SQUOTE] = ACTIONS(402),
    [anon_sym_BQUOTE] = ACTIONS(402),
    [anon_sym_POUND_SQUOTE] = ACTIONS(402),
    [anon_sym_POUND_BQUOTE] = ACTIONS(402),
    [anon_sym_COMMA] = ACTIONS(404),
    [anon_sym_COMMA_AT] = ACTIONS(402),
    [anon_sym_POUND_COMMA] = ACTIONS(404),
    [anon_sym_POUND_COMMA_AT] = ACTIONS(402),
    [anon_sym_POUND_LPAREN] = ACTIONS(402),
    [anon_sym_POUNDvu8_LPAREN] = ACTIONS(402),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 2,
    ACTIONS(408), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(406), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [33] = 2,
    ACTIONS(412), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(410), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [66] = 2,
    ACTIONS(416), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(414), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [99] = 2,
    ACTIONS(420), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(418), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [132] = 2,
    ACTIONS(424), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(422), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [165] = 2,
    ACTIONS(428), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(426), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [198] = 2,
    ACTIONS(432), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(430), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [231] = 2,
    ACTIONS(436), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(434), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [264] = 2,
    ACTIONS(440), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(438), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [297] = 2,
    ACTIONS(444), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(442), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [330] = 2,
    ACTIONS(448), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(446), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [363] = 2,
    ACTIONS(452), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(450), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [396] = 2,
    ACTIONS(456), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(454), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [429] = 2,
    ACTIONS(460), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(458), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [462] = 2,
    ACTIONS(464), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(462), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [495] = 2,
    ACTIONS(468), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(466), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [528] = 2,
    ACTIONS(472), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(470), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [561] = 2,
    ACTIONS(476), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(474), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [594] = 2,
    ACTIONS(480), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(478), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [627] = 2,
    ACTIONS(484), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(482), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [660] = 2,
    ACTIONS(488), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(486), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [693] = 2,
    ACTIONS(492), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(490), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [726] = 2,
    ACTIONS(496), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(494), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [759] = 2,
    ACTIONS(500), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(498), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [792] = 2,
    ACTIONS(504), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(502), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [825] = 2,
    ACTIONS(508), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(506), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [858] = 2,
    ACTIONS(512), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(510), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [891] = 2,
    ACTIONS(516), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(514), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [924] = 2,
    ACTIONS(520), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(518), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [957] = 2,
    ACTIONS(524), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(522), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [990] = 2,
    ACTIONS(528), 4,
      sym_number,
      sym_symbol,
      anon_sym_COMMA,
      anon_sym_POUND_COMMA,
    ACTIONS(526), 24,
      ts_builtin_sym_end,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      anon_sym_POUND_PIPE,
      sym_boolean,
      sym_character,
      anon_sym_DQUOTE,
      sym_keyword,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_LBRACK,
      anon_sym_RBRACK,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      anon_sym_SQUOTE,
      anon_sym_BQUOTE,
      anon_sym_POUND_SQUOTE,
      anon_sym_POUND_BQUOTE,
      anon_sym_COMMA_AT,
      anon_sym_POUND_COMMA_AT,
      anon_sym_POUND_LPAREN,
      anon_sym_POUNDvu8_LPAREN,
  [1023] = 7,
    ACTIONS(530), 1,
      aux_sym__intertoken_token1,
    ACTIONS(532), 1,
      aux_sym_comment_token1,
    ACTIONS(534), 1,
      anon_sym_POUND_SEMI,
    ACTIONS(536), 1,
      anon_sym_POUND_BANG,
    ACTIONS(538), 1,
      aux_sym_directive_token1,
    ACTIONS(540), 1,
      anon_sym_POUND_PIPE,
    STATE(94), 5,
      sym__intertoken,
      sym_comment,
      sym_directive,
      sym_block_comment,
      aux_sym_comment_repeat1,
  [1049] = 7,
    ACTIONS(532), 1,
      aux_sym_comment_token1,
    ACTIONS(534), 1,
      anon_sym_POUND_SEMI,
    ACTIONS(536), 1,
      anon_sym_POUND_BANG,
    ACTIONS(540), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(542), 1,
      aux_sym__intertoken_token1,
    ACTIONS(544), 1,
      aux_sym_directive_token1,
    STATE(92), 5,
      sym__intertoken,
      sym_comment,
      sym_directive,
      sym_block_comment,
      aux_sym_comment_repeat1,
  [1075] = 7,
    ACTIONS(402), 1,
      aux_sym_directive_token1,
    ACTIONS(546), 1,
      aux_sym__intertoken_token1,
    ACTIONS(549), 1,
      aux_sym_comment_token1,
    ACTIONS(552), 1,
      anon_sym_POUND_SEMI,
    ACTIONS(555), 1,
      anon_sym_POUND_BANG,
    ACTIONS(558), 1,
      anon_sym_POUND_PIPE,
    STATE(94), 5,
      sym__intertoken,
      sym_comment,
      sym_directive,
      sym_block_comment,
      aux_sym_comment_repeat1,
  [1101] = 7,
    ACTIONS(532), 1,
      aux_sym_comment_token1,
    ACTIONS(534), 1,
      anon_sym_POUND_SEMI,
    ACTIONS(536), 1,
      anon_sym_POUND_BANG,
    ACTIONS(540), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(561), 1,
      aux_sym__intertoken_token1,
    ACTIONS(563), 1,
      aux_sym_directive_token1,
    STATE(96), 5,
      sym__intertoken,
      sym_comment,
      sym_directive,
      sym_block_comment,
      aux_sym_comment_repeat1,
  [1127] = 7,
    ACTIONS(530), 1,
      aux_sym__intertoken_token1,
    ACTIONS(532), 1,
      aux_sym_comment_token1,
    ACTIONS(534), 1,
      anon_sym_POUND_SEMI,
    ACTIONS(536), 1,
      anon_sym_POUND_BANG,
    ACTIONS(540), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(565), 1,
      aux_sym_directive_token1,
    STATE(94), 5,
      sym__intertoken,
      sym_comment,
      sym_directive,
      sym_block_comment,
      aux_sym_comment_repeat1,
  [1153] = 1,
    ACTIONS(458), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1162] = 1,
    ACTIONS(454), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1171] = 1,
    ACTIONS(482), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1180] = 1,
    ACTIONS(462), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1189] = 1,
    ACTIONS(486), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1198] = 1,
    ACTIONS(466), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1207] = 1,
    ACTIONS(494), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1216] = 1,
    ACTIONS(498), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1225] = 1,
    ACTIONS(506), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1234] = 1,
    ACTIONS(514), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1243] = 1,
    ACTIONS(518), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1252] = 1,
    ACTIONS(522), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1261] = 1,
    ACTIONS(422), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1270] = 1,
    ACTIONS(410), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1279] = 1,
    ACTIONS(414), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1288] = 1,
    ACTIONS(418), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1297] = 1,
    ACTIONS(526), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1306] = 1,
    ACTIONS(426), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1315] = 1,
    ACTIONS(406), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1324] = 1,
    ACTIONS(430), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1333] = 1,
    ACTIONS(434), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1342] = 1,
    ACTIONS(438), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1351] = 1,
    ACTIONS(442), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1360] = 1,
    ACTIONS(470), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1369] = 1,
    ACTIONS(478), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1378] = 1,
    ACTIONS(490), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1387] = 1,
    ACTIONS(502), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1396] = 1,
    ACTIONS(446), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1405] = 1,
    ACTIONS(474), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1414] = 1,
    ACTIONS(450), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1423] = 1,
    ACTIONS(510), 6,
      aux_sym__intertoken_token1,
      aux_sym_comment_token1,
      anon_sym_POUND_SEMI,
      anon_sym_POUND_BANG,
      aux_sym_directive_token1,
      anon_sym_POUND_PIPE,
  [1432] = 4,
    ACTIONS(567), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(569), 1,
      aux_sym_block_comment_token1,
    ACTIONS(571), 1,
      anon_sym_PIPE_POUND,
    STATE(130), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1446] = 4,
    ACTIONS(573), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(576), 1,
      aux_sym_block_comment_token1,
    ACTIONS(579), 1,
      anon_sym_PIPE_POUND,
    STATE(129), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1460] = 4,
    ACTIONS(567), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(581), 1,
      aux_sym_block_comment_token1,
    ACTIONS(583), 1,
      anon_sym_PIPE_POUND,
    STATE(129), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1474] = 4,
    ACTIONS(567), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(581), 1,
      aux_sym_block_comment_token1,
    ACTIONS(585), 1,
      anon_sym_PIPE_POUND,
    STATE(129), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1488] = 4,
    ACTIONS(567), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(587), 1,
      aux_sym_block_comment_token1,
    ACTIONS(589), 1,
      anon_sym_PIPE_POUND,
    STATE(131), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1502] = 4,
    ACTIONS(567), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(591), 1,
      aux_sym_block_comment_token1,
    ACTIONS(593), 1,
      anon_sym_PIPE_POUND,
    STATE(134), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1516] = 4,
    ACTIONS(567), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(581), 1,
      aux_sym_block_comment_token1,
    ACTIONS(595), 1,
      anon_sym_PIPE_POUND,
    STATE(129), 2,
      sym_block_comment,
      aux_sym_block_comment_repeat1,
  [1530] = 3,
    ACTIONS(597), 1,
      anon_sym_DQUOTE,
    STATE(136), 1,
      aux_sym_string_repeat1,
    ACTIONS(599), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [1541] = 3,
    ACTIONS(601), 1,
      anon_sym_DQUOTE,
    STATE(137), 1,
      aux_sym_string_repeat1,
    ACTIONS(603), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [1552] = 3,
    ACTIONS(605), 1,
      anon_sym_DQUOTE,
    STATE(137), 1,
      aux_sym_string_repeat1,
    ACTIONS(607), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [1563] = 3,
    ACTIONS(610), 1,
      anon_sym_DQUOTE,
    STATE(137), 1,
      aux_sym_string_repeat1,
    ACTIONS(603), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [1574] = 3,
    ACTIONS(612), 1,
      anon_sym_DQUOTE,
    STATE(138), 1,
      aux_sym_string_repeat1,
    ACTIONS(614), 2,
      aux_sym_string_token1,
      sym_escape_sequence,
  [1585] = 2,
    ACTIONS(520), 1,
      aux_sym_block_comment_token1,
    ACTIONS(518), 2,
      anon_sym_POUND_PIPE,
      anon_sym_PIPE_POUND,
  [1593] = 2,
    ACTIONS(480), 1,
      aux_sym_block_comment_token1,
    ACTIONS(478), 2,
      anon_sym_POUND_PIPE,
      anon_sym_PIPE_POUND,
  [1601] = 1,
    ACTIONS(616), 1,
      ts_builtin_sym_end,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(61)] = 0,
  [SMALL_STATE(62)] = 33,
  [SMALL_STATE(63)] = 66,
  [SMALL_STATE(64)] = 99,
  [SMALL_STATE(65)] = 132,
  [SMALL_STATE(66)] = 165,
  [SMALL_STATE(67)] = 198,
  [SMALL_STATE(68)] = 231,
  [SMALL_STATE(69)] = 264,
  [SMALL_STATE(70)] = 297,
  [SMALL_STATE(71)] = 330,
  [SMALL_STATE(72)] = 363,
  [SMALL_STATE(73)] = 396,
  [SMALL_STATE(74)] = 429,
  [SMALL_STATE(75)] = 462,
  [SMALL_STATE(76)] = 495,
  [SMALL_STATE(77)] = 528,
  [SMALL_STATE(78)] = 561,
  [SMALL_STATE(79)] = 594,
  [SMALL_STATE(80)] = 627,
  [SMALL_STATE(81)] = 660,
  [SMALL_STATE(82)] = 693,
  [SMALL_STATE(83)] = 726,
  [SMALL_STATE(84)] = 759,
  [SMALL_STATE(85)] = 792,
  [SMALL_STATE(86)] = 825,
  [SMALL_STATE(87)] = 858,
  [SMALL_STATE(88)] = 891,
  [SMALL_STATE(89)] = 924,
  [SMALL_STATE(90)] = 957,
  [SMALL_STATE(91)] = 990,
  [SMALL_STATE(92)] = 1023,
  [SMALL_STATE(93)] = 1049,
  [SMALL_STATE(94)] = 1075,
  [SMALL_STATE(95)] = 1101,
  [SMALL_STATE(96)] = 1127,
  [SMALL_STATE(97)] = 1153,
  [SMALL_STATE(98)] = 1162,
  [SMALL_STATE(99)] = 1171,
  [SMALL_STATE(100)] = 1180,
  [SMALL_STATE(101)] = 1189,
  [SMALL_STATE(102)] = 1198,
  [SMALL_STATE(103)] = 1207,
  [SMALL_STATE(104)] = 1216,
  [SMALL_STATE(105)] = 1225,
  [SMALL_STATE(106)] = 1234,
  [SMALL_STATE(107)] = 1243,
  [SMALL_STATE(108)] = 1252,
  [SMALL_STATE(109)] = 1261,
  [SMALL_STATE(110)] = 1270,
  [SMALL_STATE(111)] = 1279,
  [SMALL_STATE(112)] = 1288,
  [SMALL_STATE(113)] = 1297,
  [SMALL_STATE(114)] = 1306,
  [SMALL_STATE(115)] = 1315,
  [SMALL_STATE(116)] = 1324,
  [SMALL_STATE(117)] = 1333,
  [SMALL_STATE(118)] = 1342,
  [SMALL_STATE(119)] = 1351,
  [SMALL_STATE(120)] = 1360,
  [SMALL_STATE(121)] = 1369,
  [SMALL_STATE(122)] = 1378,
  [SMALL_STATE(123)] = 1387,
  [SMALL_STATE(124)] = 1396,
  [SMALL_STATE(125)] = 1405,
  [SMALL_STATE(126)] = 1414,
  [SMALL_STATE(127)] = 1423,
  [SMALL_STATE(128)] = 1432,
  [SMALL_STATE(129)] = 1446,
  [SMALL_STATE(130)] = 1460,
  [SMALL_STATE(131)] = 1474,
  [SMALL_STATE(132)] = 1488,
  [SMALL_STATE(133)] = 1502,
  [SMALL_STATE(134)] = 1516,
  [SMALL_STATE(135)] = 1530,
  [SMALL_STATE(136)] = 1541,
  [SMALL_STATE(137)] = 1552,
  [SMALL_STATE(138)] = 1563,
  [SMALL_STATE(139)] = 1574,
  [SMALL_STATE(140)] = 1585,
  [SMALL_STATE(141)] = 1593,
  [SMALL_STATE(142)] = 1601,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_program, 0, 0, 0),
  [5] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(87),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(93),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(128),
  [15] = {.entry = {.count = 1, .reusable = false}}, SHIFT(7),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(135),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(13),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(38),
  [27] = {.entry = {.count = 1, .reusable = true}}, SHIFT(28),
  [29] = {.entry = {.count = 1, .reusable = true}}, SHIFT(29),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(49),
  [33] = {.entry = {.count = 1, .reusable = false}}, SHIFT(51),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(52),
  [37] = {.entry = {.count = 1, .reusable = false}}, SHIFT(53),
  [39] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [41] = {.entry = {.count = 1, .reusable = true}}, SHIFT(5),
  [43] = {.entry = {.count = 1, .reusable = true}}, SHIFT(6),
  [45] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0),
  [47] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(2),
  [50] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(87),
  [53] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(30),
  [56] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(93),
  [59] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(128),
  [62] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(2),
  [65] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(135),
  [68] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(11),
  [71] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(13),
  [74] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(23),
  [77] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(38),
  [80] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(28),
  [83] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(29),
  [86] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(49),
  [89] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(51),
  [92] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(52),
  [95] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(53),
  [98] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(27),
  [101] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(5),
  [104] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(6),
  [107] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [109] = {.entry = {.count = 1, .reusable = false}}, SHIFT(2),
  [111] = {.entry = {.count = 1, .reusable = true}}, SHIFT(69),
  [113] = {.entry = {.count = 1, .reusable = true}}, SHIFT(119),
  [115] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [117] = {.entry = {.count = 1, .reusable = false}}, SHIFT(3),
  [119] = {.entry = {.count = 1, .reusable = true}}, SHIFT(83),
  [121] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [123] = {.entry = {.count = 1, .reusable = false}}, SHIFT(12),
  [125] = {.entry = {.count = 1, .reusable = true}}, SHIFT(84),
  [127] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_program, 1, 0, 0),
  [129] = {.entry = {.count = 1, .reusable = true}}, SHIFT(65),
  [131] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [133] = {.entry = {.count = 1, .reusable = false}}, SHIFT(8),
  [135] = {.entry = {.count = 1, .reusable = true}}, SHIFT(85),
  [137] = {.entry = {.count = 1, .reusable = true}}, SHIFT(70),
  [139] = {.entry = {.count = 1, .reusable = true}}, SHIFT(9),
  [141] = {.entry = {.count = 1, .reusable = false}}, SHIFT(9),
  [143] = {.entry = {.count = 1, .reusable = true}}, SHIFT(19),
  [145] = {.entry = {.count = 1, .reusable = false}}, SHIFT(19),
  [147] = {.entry = {.count = 1, .reusable = true}}, SHIFT(123),
  [149] = {.entry = {.count = 1, .reusable = true}}, SHIFT(20),
  [151] = {.entry = {.count = 1, .reusable = false}}, SHIFT(20),
  [153] = {.entry = {.count = 1, .reusable = true}}, SHIFT(21),
  [155] = {.entry = {.count = 1, .reusable = false}}, SHIFT(21),
  [157] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [159] = {.entry = {.count = 1, .reusable = false}}, SHIFT(22),
  [161] = {.entry = {.count = 1, .reusable = true}}, SHIFT(103),
  [163] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [165] = {.entry = {.count = 1, .reusable = false}}, SHIFT(4),
  [167] = {.entry = {.count = 1, .reusable = true}}, SHIFT(104),
  [169] = {.entry = {.count = 1, .reusable = true}}, SHIFT(109),
  [171] = {.entry = {.count = 1, .reusable = true}}, SHIFT(118),
  [173] = {.entry = {.count = 1, .reusable = true}}, SHIFT(10),
  [175] = {.entry = {.count = 1, .reusable = false}}, SHIFT(10),
  [177] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [179] = {.entry = {.count = 1, .reusable = true}}, SHIFT(101),
  [181] = {.entry = {.count = 1, .reusable = false}}, SHIFT(101),
  [183] = {.entry = {.count = 1, .reusable = true}}, SHIFT(139),
  [185] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [187] = {.entry = {.count = 1, .reusable = true}}, SHIFT(15),
  [189] = {.entry = {.count = 1, .reusable = true}}, SHIFT(16),
  [191] = {.entry = {.count = 1, .reusable = true}}, SHIFT(31),
  [193] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [195] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [197] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [199] = {.entry = {.count = 1, .reusable = false}}, SHIFT(35),
  [201] = {.entry = {.count = 1, .reusable = true}}, SHIFT(36),
  [203] = {.entry = {.count = 1, .reusable = false}}, SHIFT(37),
  [205] = {.entry = {.count = 1, .reusable = true}}, SHIFT(24),
  [207] = {.entry = {.count = 1, .reusable = true}}, SHIFT(17),
  [209] = {.entry = {.count = 1, .reusable = true}}, SHIFT(18),
  [211] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [213] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [215] = {.entry = {.count = 1, .reusable = false}}, SHIFT(67),
  [217] = {.entry = {.count = 1, .reusable = true}}, SHIFT(68),
  [219] = {.entry = {.count = 1, .reusable = false}}, SHIFT(68),
  [221] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [223] = {.entry = {.count = 1, .reusable = true}}, SHIFT(81),
  [225] = {.entry = {.count = 1, .reusable = false}}, SHIFT(81),
  [227] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [229] = {.entry = {.count = 1, .reusable = true}}, SHIFT(72),
  [231] = {.entry = {.count = 1, .reusable = false}}, SHIFT(72),
  [233] = {.entry = {.count = 1, .reusable = true}}, SHIFT(56),
  [235] = {.entry = {.count = 1, .reusable = true}}, SHIFT(74),
  [237] = {.entry = {.count = 1, .reusable = false}}, SHIFT(74),
  [239] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [241] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [243] = {.entry = {.count = 1, .reusable = false}}, SHIFT(73),
  [245] = {.entry = {.count = 1, .reusable = true}}, SHIFT(40),
  [247] = {.entry = {.count = 1, .reusable = true}}, SHIFT(124),
  [249] = {.entry = {.count = 1, .reusable = false}}, SHIFT(124),
  [251] = {.entry = {.count = 1, .reusable = true}}, SHIFT(41),
  [253] = {.entry = {.count = 1, .reusable = true}}, SHIFT(126),
  [255] = {.entry = {.count = 1, .reusable = false}}, SHIFT(126),
  [257] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [259] = {.entry = {.count = 1, .reusable = true}}, SHIFT(97),
  [261] = {.entry = {.count = 1, .reusable = false}}, SHIFT(97),
  [263] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [265] = {.entry = {.count = 1, .reusable = true}}, SHIFT(100),
  [267] = {.entry = {.count = 1, .reusable = false}}, SHIFT(100),
  [269] = {.entry = {.count = 1, .reusable = true}}, SHIFT(44),
  [271] = {.entry = {.count = 1, .reusable = true}}, SHIFT(120),
  [273] = {.entry = {.count = 1, .reusable = false}}, SHIFT(120),
  [275] = {.entry = {.count = 1, .reusable = true}}, SHIFT(45),
  [277] = {.entry = {.count = 1, .reusable = true}}, SHIFT(125),
  [279] = {.entry = {.count = 1, .reusable = false}}, SHIFT(125),
  [281] = {.entry = {.count = 1, .reusable = true}}, SHIFT(46),
  [283] = {.entry = {.count = 1, .reusable = true}}, SHIFT(99),
  [285] = {.entry = {.count = 1, .reusable = false}}, SHIFT(99),
  [287] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [289] = {.entry = {.count = 1, .reusable = true}}, SHIFT(71),
  [291] = {.entry = {.count = 1, .reusable = false}}, SHIFT(71),
  [293] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [295] = {.entry = {.count = 1, .reusable = false}}, SHIFT(61),
  [297] = {.entry = {.count = 1, .reusable = true}}, SHIFT(110),
  [299] = {.entry = {.count = 1, .reusable = false}}, SHIFT(110),
  [301] = {.entry = {.count = 1, .reusable = true}}, SHIFT(111),
  [303] = {.entry = {.count = 1, .reusable = false}}, SHIFT(111),
  [305] = {.entry = {.count = 1, .reusable = true}}, SHIFT(112),
  [307] = {.entry = {.count = 1, .reusable = false}}, SHIFT(112),
  [309] = {.entry = {.count = 1, .reusable = true}}, SHIFT(113),
  [311] = {.entry = {.count = 1, .reusable = false}}, SHIFT(113),
  [313] = {.entry = {.count = 1, .reusable = true}}, SHIFT(114),
  [315] = {.entry = {.count = 1, .reusable = false}}, SHIFT(114),
  [317] = {.entry = {.count = 1, .reusable = true}}, SHIFT(115),
  [319] = {.entry = {.count = 1, .reusable = false}}, SHIFT(115),
  [321] = {.entry = {.count = 1, .reusable = true}}, SHIFT(116),
  [323] = {.entry = {.count = 1, .reusable = false}}, SHIFT(116),
  [325] = {.entry = {.count = 1, .reusable = true}}, SHIFT(117),
  [327] = {.entry = {.count = 1, .reusable = false}}, SHIFT(117),
  [329] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [331] = {.entry = {.count = 1, .reusable = true}}, SHIFT(98),
  [333] = {.entry = {.count = 1, .reusable = false}}, SHIFT(98),
  [335] = {.entry = {.count = 1, .reusable = true}}, SHIFT(57),
  [337] = {.entry = {.count = 1, .reusable = true}}, SHIFT(75),
  [339] = {.entry = {.count = 1, .reusable = false}}, SHIFT(75),
  [341] = {.entry = {.count = 1, .reusable = true}}, SHIFT(86),
  [343] = {.entry = {.count = 1, .reusable = false}}, SHIFT(86),
  [345] = {.entry = {.count = 1, .reusable = true}}, SHIFT(58),
  [347] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [349] = {.entry = {.count = 1, .reusable = false}}, SHIFT(77),
  [351] = {.entry = {.count = 1, .reusable = true}}, SHIFT(39),
  [353] = {.entry = {.count = 1, .reusable = true}}, SHIFT(78),
  [355] = {.entry = {.count = 1, .reusable = false}}, SHIFT(78),
  [357] = {.entry = {.count = 1, .reusable = true}}, SHIFT(25),
  [359] = {.entry = {.count = 1, .reusable = true}}, SHIFT(80),
  [361] = {.entry = {.count = 1, .reusable = false}}, SHIFT(80),
  [363] = {.entry = {.count = 1, .reusable = true}}, SHIFT(62),
  [365] = {.entry = {.count = 1, .reusable = false}}, SHIFT(62),
  [367] = {.entry = {.count = 1, .reusable = true}}, SHIFT(63),
  [369] = {.entry = {.count = 1, .reusable = false}}, SHIFT(63),
  [371] = {.entry = {.count = 1, .reusable = true}}, SHIFT(64),
  [373] = {.entry = {.count = 1, .reusable = false}}, SHIFT(64),
  [375] = {.entry = {.count = 1, .reusable = true}}, SHIFT(91),
  [377] = {.entry = {.count = 1, .reusable = false}}, SHIFT(91),
  [379] = {.entry = {.count = 1, .reusable = true}}, SHIFT(66),
  [381] = {.entry = {.count = 1, .reusable = false}}, SHIFT(66),
  [383] = {.entry = {.count = 1, .reusable = true}}, SHIFT(105),
  [385] = {.entry = {.count = 1, .reusable = false}}, SHIFT(105),
  [387] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(60),
  [390] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(87),
  [393] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(30),
  [396] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(93),
  [399] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(128),
  [402] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0),
  [404] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0),
  [406] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unquote_splicing, 3, 0, 0),
  [408] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unquote_splicing, 3, 0, 0),
  [410] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_quote, 3, 0, 0),
  [412] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_quote, 3, 0, 0),
  [414] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_quasiquote, 3, 0, 0),
  [416] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_quasiquote, 3, 0, 0),
  [418] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_syntax, 3, 0, 0),
  [420] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_syntax, 3, 0, 0),
  [422] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 3, 0, 0),
  [424] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_list, 3, 0, 0),
  [426] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unquote, 3, 0, 0),
  [428] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unquote, 3, 0, 0),
  [430] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unsyntax, 3, 0, 0),
  [432] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unsyntax, 3, 0, 0),
  [434] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unsyntax_splicing, 3, 0, 0),
  [436] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unsyntax_splicing, 3, 0, 0),
  [438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_vector, 3, 0, 0),
  [440] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_vector, 3, 0, 0),
  [442] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_byte_vector, 3, 0, 0),
  [444] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_byte_vector, 3, 0, 0),
  [446] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_quote, 2, 0, 0),
  [448] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_quote, 2, 0, 0),
  [450] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_quasiquote, 2, 0, 0),
  [452] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_quasiquote, 2, 0, 0),
  [454] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comment, 2, 0, 0),
  [456] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comment, 2, 0, 0),
  [458] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_syntax, 2, 0, 0),
  [460] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_syntax, 2, 0, 0),
  [462] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_quasisyntax, 2, 0, 0),
  [464] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_quasisyntax, 2, 0, 0),
  [466] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_directive, 2, 0, 0),
  [468] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_directive, 2, 0, 0),
  [470] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unquote, 2, 0, 0),
  [472] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unquote, 2, 0, 0),
  [474] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unquote_splicing, 2, 0, 0),
  [476] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unquote_splicing, 2, 0, 0),
  [478] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_block_comment, 2, 0, 0),
  [480] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_block_comment, 2, 0, 0),
  [482] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unsyntax, 2, 0, 0),
  [484] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unsyntax, 2, 0, 0),
  [486] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unsyntax_splicing, 2, 0, 0),
  [488] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unsyntax_splicing, 2, 0, 0),
  [490] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 2, 0, 0),
  [492] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 2, 0, 0),
  [494] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_vector, 2, 0, 0),
  [496] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_vector, 2, 0, 0),
  [498] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_byte_vector, 2, 0, 0),
  [500] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_byte_vector, 2, 0, 0),
  [502] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 2, 0, 0),
  [504] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_list, 2, 0, 0),
  [506] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comment, 3, 0, 0),
  [508] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comment, 3, 0, 0),
  [510] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comment, 1, 0, 0),
  [512] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comment, 1, 0, 0),
  [514] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_directive, 3, 0, 0),
  [516] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_directive, 3, 0, 0),
  [518] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_block_comment, 3, 0, 0),
  [520] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_block_comment, 3, 0, 0),
  [522] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 3, 0, 0),
  [524] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 3, 0, 0),
  [526] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_quasisyntax, 3, 0, 0),
  [528] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_quasisyntax, 3, 0, 0),
  [530] = {.entry = {.count = 1, .reusable = true}}, SHIFT(94),
  [532] = {.entry = {.count = 1, .reusable = true}}, SHIFT(127),
  [534] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [536] = {.entry = {.count = 1, .reusable = true}}, SHIFT(95),
  [538] = {.entry = {.count = 1, .reusable = true}}, SHIFT(88),
  [540] = {.entry = {.count = 1, .reusable = true}}, SHIFT(132),
  [542] = {.entry = {.count = 1, .reusable = true}}, SHIFT(92),
  [544] = {.entry = {.count = 1, .reusable = true}}, SHIFT(76),
  [546] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(94),
  [549] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(127),
  [552] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(48),
  [555] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(95),
  [558] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(132),
  [561] = {.entry = {.count = 1, .reusable = true}}, SHIFT(96),
  [563] = {.entry = {.count = 1, .reusable = true}}, SHIFT(102),
  [565] = {.entry = {.count = 1, .reusable = true}}, SHIFT(106),
  [567] = {.entry = {.count = 1, .reusable = true}}, SHIFT(133),
  [569] = {.entry = {.count = 1, .reusable = false}}, SHIFT(130),
  [571] = {.entry = {.count = 1, .reusable = true}}, SHIFT(79),
  [573] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_block_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(133),
  [576] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_block_comment_repeat1, 2, 0, 0), SHIFT_REPEAT(129),
  [579] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_block_comment_repeat1, 2, 0, 0),
  [581] = {.entry = {.count = 1, .reusable = false}}, SHIFT(129),
  [583] = {.entry = {.count = 1, .reusable = true}}, SHIFT(89),
  [585] = {.entry = {.count = 1, .reusable = true}}, SHIFT(107),
  [587] = {.entry = {.count = 1, .reusable = false}}, SHIFT(131),
  [589] = {.entry = {.count = 1, .reusable = true}}, SHIFT(121),
  [591] = {.entry = {.count = 1, .reusable = false}}, SHIFT(134),
  [593] = {.entry = {.count = 1, .reusable = true}}, SHIFT(141),
  [595] = {.entry = {.count = 1, .reusable = true}}, SHIFT(140),
  [597] = {.entry = {.count = 1, .reusable = true}}, SHIFT(82),
  [599] = {.entry = {.count = 1, .reusable = true}}, SHIFT(136),
  [601] = {.entry = {.count = 1, .reusable = true}}, SHIFT(90),
  [603] = {.entry = {.count = 1, .reusable = true}}, SHIFT(137),
  [605] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0),
  [607] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0), SHIFT_REPEAT(137),
  [610] = {.entry = {.count = 1, .reusable = true}}, SHIFT(108),
  [612] = {.entry = {.count = 1, .reusable = true}}, SHIFT(122),
  [614] = {.entry = {.count = 1, .reusable = true}}, SHIFT(138),
  [616] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
};

#ifdef __cplusplus
extern "C" {
#endif
#ifdef TREE_SITTER_HIDE_SYMBOLS
#define TS_PUBLIC
#elif defined(_WIN32)
#define TS_PUBLIC __declspec(dllexport)
#else
#define TS_PUBLIC __attribute__((visibility("default")))
#endif

TS_PUBLIC const TSLanguage *tree_sitter_scheme(void) {
  static const TSLanguage language = {
    .version = LANGUAGE_VERSION,
    .symbol_count = SYMBOL_COUNT,
    .alias_count = ALIAS_COUNT,
    .token_count = TOKEN_COUNT,
    .external_token_count = EXTERNAL_TOKEN_COUNT,
    .state_count = STATE_COUNT,
    .large_state_count = LARGE_STATE_COUNT,
    .production_id_count = PRODUCTION_ID_COUNT,
    .field_count = FIELD_COUNT,
    .max_alias_sequence_length = MAX_ALIAS_SEQUENCE_LENGTH,
    .parse_table = &ts_parse_table[0][0],
    .small_parse_table = ts_small_parse_table,
    .small_parse_table_map = ts_small_parse_table_map,
    .parse_actions = ts_parse_actions,
    .symbol_names = ts_symbol_names,
    .symbol_metadata = ts_symbol_metadata,
    .public_symbol_map = ts_symbol_map,
    .alias_map = ts_non_terminal_alias_map,
    .alias_sequences = &ts_alias_sequences[0][0],
    .lex_modes = ts_lex_modes,
    .lex_fn = ts_lex,
    .primary_state_ids = ts_primary_state_ids,
  };
  return &language;
}
#ifdef __cplusplus
}
#endif
