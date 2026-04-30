#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 137
#define LARGE_STATE_COUNT 3
#define SYMBOL_COUNT 65
#define ALIAS_COUNT 0
#define TOKEN_COUNT 41
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 6
#define MAX_ALIAS_SEQUENCE_LENGTH 7
#define PRODUCTION_ID_COUNT 6

enum ts_symbol_identifiers {
  anon_sym_LF = 1,
  anon_sym_COMMA = 2,
  anon_sym_COLON = 3,
  anon_sym_LPAREN = 4,
  anon_sym_RPAREN = 5,
  anon_sym_label = 6,
  anon_sym_const = 7,
  anon_sym_LBRACE = 8,
  anon_sym_DASH = 9,
  anon_sym_RBRACE = 10,
  anon_sym_byte = 11,
  anon_sym_word = 12,
  anon_sym_dword = 13,
  anon_sym_qword = 14,
  anon_sym_ptr = 15,
  anon_sym_LBRACK = 16,
  anon_sym_PLUS = 17,
  anon_sym_RBRACK = 18,
  anon_sym_STAR = 19,
  anon_sym_rel = 20,
  anon_sym_BANG = 21,
  anon_sym_SLASH = 22,
  anon_sym_PERCENT = 23,
  anon_sym_PIPE = 24,
  anon_sym_CARET = 25,
  anon_sym_AMP = 26,
  anon_sym_POUND = 27,
  aux_sym_int_token1 = 28,
  aux_sym_int_token2 = 29,
  sym_float = 30,
  aux_sym_string_token1 = 31,
  aux_sym_string_token2 = 32,
  sym_word = 33,
  sym__reg = 34,
  sym_address = 35,
  sym_meta_ident = 36,
  sym__ident = 37,
  aux_sym_line_comment_token1 = 38,
  aux_sym_line_comment_token2 = 39,
  sym_block_comment = 40,
  sym_program = 41,
  sym__item = 42,
  sym_meta = 43,
  sym_label = 44,
  sym_const = 45,
  sym_instruction = 46,
  sym__expr = 47,
  sym_list = 48,
  sym_ptr = 49,
  sym__tc_expr = 50,
  sym_tc_infix = 51,
  sym_int = 52,
  sym_string = 53,
  sym_reg = 54,
  sym_ident = 55,
  sym_line_comment = 56,
  aux_sym_program_repeat1 = 57,
  aux_sym_program_repeat2 = 58,
  aux_sym_meta_repeat1 = 59,
  aux_sym_meta_repeat2 = 60,
  aux_sym_meta_repeat3 = 61,
  aux_sym_instruction_repeat1 = 62,
  aux_sym_instruction_repeat2 = 63,
  aux_sym_list_repeat1 = 64,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_LF] = "\n",
  [anon_sym_COMMA] = ",",
  [anon_sym_COLON] = ":",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [anon_sym_label] = "label",
  [anon_sym_const] = "const",
  [anon_sym_LBRACE] = "{",
  [anon_sym_DASH] = "-",
  [anon_sym_RBRACE] = "}",
  [anon_sym_byte] = "byte",
  [anon_sym_word] = "word",
  [anon_sym_dword] = "dword",
  [anon_sym_qword] = "qword",
  [anon_sym_ptr] = "ptr",
  [anon_sym_LBRACK] = "[",
  [anon_sym_PLUS] = "+",
  [anon_sym_RBRACK] = "]",
  [anon_sym_STAR] = "*",
  [anon_sym_rel] = "rel",
  [anon_sym_BANG] = "!",
  [anon_sym_SLASH] = "/",
  [anon_sym_PERCENT] = "%",
  [anon_sym_PIPE] = "|",
  [anon_sym_CARET] = "^",
  [anon_sym_AMP] = "&",
  [anon_sym_POUND] = "#",
  [aux_sym_int_token1] = "int_token1",
  [aux_sym_int_token2] = "int_token2",
  [sym_float] = "float",
  [aux_sym_string_token1] = "string_token1",
  [aux_sym_string_token2] = "string_token2",
  [sym_word] = "word",
  [sym__reg] = "_reg",
  [sym_address] = "address",
  [sym_meta_ident] = "meta_ident",
  [sym__ident] = "_ident",
  [aux_sym_line_comment_token1] = "line_comment_token1",
  [aux_sym_line_comment_token2] = "line_comment_token2",
  [sym_block_comment] = "block_comment",
  [sym_program] = "program",
  [sym__item] = "_item",
  [sym_meta] = "meta",
  [sym_label] = "label",
  [sym_const] = "const",
  [sym_instruction] = "instruction",
  [sym__expr] = "_expr",
  [sym_list] = "list",
  [sym_ptr] = "ptr",
  [sym__tc_expr] = "_tc_expr",
  [sym_tc_infix] = "tc_infix",
  [sym_int] = "int",
  [sym_string] = "string",
  [sym_reg] = "reg",
  [sym_ident] = "ident",
  [sym_line_comment] = "line_comment",
  [aux_sym_program_repeat1] = "program_repeat1",
  [aux_sym_program_repeat2] = "program_repeat2",
  [aux_sym_meta_repeat1] = "meta_repeat1",
  [aux_sym_meta_repeat2] = "meta_repeat2",
  [aux_sym_meta_repeat3] = "meta_repeat3",
  [aux_sym_instruction_repeat1] = "instruction_repeat1",
  [aux_sym_instruction_repeat2] = "instruction_repeat2",
  [aux_sym_list_repeat1] = "list_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_LF] = anon_sym_LF,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [anon_sym_COLON] = anon_sym_COLON,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [anon_sym_label] = anon_sym_label,
  [anon_sym_const] = anon_sym_const,
  [anon_sym_LBRACE] = anon_sym_LBRACE,
  [anon_sym_DASH] = anon_sym_DASH,
  [anon_sym_RBRACE] = anon_sym_RBRACE,
  [anon_sym_byte] = anon_sym_byte,
  [anon_sym_word] = anon_sym_word,
  [anon_sym_dword] = anon_sym_dword,
  [anon_sym_qword] = anon_sym_qword,
  [anon_sym_ptr] = anon_sym_ptr,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_PLUS] = anon_sym_PLUS,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_STAR] = anon_sym_STAR,
  [anon_sym_rel] = anon_sym_rel,
  [anon_sym_BANG] = anon_sym_BANG,
  [anon_sym_SLASH] = anon_sym_SLASH,
  [anon_sym_PERCENT] = anon_sym_PERCENT,
  [anon_sym_PIPE] = anon_sym_PIPE,
  [anon_sym_CARET] = anon_sym_CARET,
  [anon_sym_AMP] = anon_sym_AMP,
  [anon_sym_POUND] = anon_sym_POUND,
  [aux_sym_int_token1] = aux_sym_int_token1,
  [aux_sym_int_token2] = aux_sym_int_token2,
  [sym_float] = sym_float,
  [aux_sym_string_token1] = aux_sym_string_token1,
  [aux_sym_string_token2] = aux_sym_string_token2,
  [sym_word] = sym_word,
  [sym__reg] = sym__reg,
  [sym_address] = sym_address,
  [sym_meta_ident] = sym_meta_ident,
  [sym__ident] = sym__ident,
  [aux_sym_line_comment_token1] = aux_sym_line_comment_token1,
  [aux_sym_line_comment_token2] = aux_sym_line_comment_token2,
  [sym_block_comment] = sym_block_comment,
  [sym_program] = sym_program,
  [sym__item] = sym__item,
  [sym_meta] = sym_meta,
  [sym_label] = sym_label,
  [sym_const] = sym_const,
  [sym_instruction] = sym_instruction,
  [sym__expr] = sym__expr,
  [sym_list] = sym_list,
  [sym_ptr] = sym_ptr,
  [sym__tc_expr] = sym__tc_expr,
  [sym_tc_infix] = sym_tc_infix,
  [sym_int] = sym_int,
  [sym_string] = sym_string,
  [sym_reg] = sym_reg,
  [sym_ident] = sym_ident,
  [sym_line_comment] = sym_line_comment,
  [aux_sym_program_repeat1] = aux_sym_program_repeat1,
  [aux_sym_program_repeat2] = aux_sym_program_repeat2,
  [aux_sym_meta_repeat1] = aux_sym_meta_repeat1,
  [aux_sym_meta_repeat2] = aux_sym_meta_repeat2,
  [aux_sym_meta_repeat3] = aux_sym_meta_repeat3,
  [aux_sym_instruction_repeat1] = aux_sym_instruction_repeat1,
  [aux_sym_instruction_repeat2] = aux_sym_instruction_repeat2,
  [aux_sym_list_repeat1] = aux_sym_list_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_LF] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COLON] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_label] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_const] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_byte] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_word] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_dword] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_qword] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_ptr] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PLUS] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_STAR] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_rel] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_BANG] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SLASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PERCENT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_CARET] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_AMP] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_int_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_int_token2] = {
    .visible = false,
    .named = false,
  },
  [sym_float] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_string_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_string_token2] = {
    .visible = false,
    .named = false,
  },
  [sym_word] = {
    .visible = true,
    .named = true,
  },
  [sym__reg] = {
    .visible = false,
    .named = true,
  },
  [sym_address] = {
    .visible = true,
    .named = true,
  },
  [sym_meta_ident] = {
    .visible = true,
    .named = true,
  },
  [sym__ident] = {
    .visible = false,
    .named = true,
  },
  [aux_sym_line_comment_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_line_comment_token2] = {
    .visible = false,
    .named = false,
  },
  [sym_block_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_program] = {
    .visible = true,
    .named = true,
  },
  [sym__item] = {
    .visible = false,
    .named = true,
  },
  [sym_meta] = {
    .visible = true,
    .named = true,
  },
  [sym_label] = {
    .visible = true,
    .named = true,
  },
  [sym_const] = {
    .visible = true,
    .named = true,
  },
  [sym_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym__expr] = {
    .visible = false,
    .named = true,
  },
  [sym_list] = {
    .visible = true,
    .named = true,
  },
  [sym_ptr] = {
    .visible = true,
    .named = true,
  },
  [sym__tc_expr] = {
    .visible = false,
    .named = true,
  },
  [sym_tc_infix] = {
    .visible = true,
    .named = true,
  },
  [sym_int] = {
    .visible = true,
    .named = true,
  },
  [sym_string] = {
    .visible = true,
    .named = true,
  },
  [sym_reg] = {
    .visible = true,
    .named = true,
  },
  [sym_ident] = {
    .visible = true,
    .named = true,
  },
  [sym_line_comment] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_program_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_program_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_meta_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_meta_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_meta_repeat3] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_instruction_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_list_repeat1] = {
    .visible = false,
    .named = false,
  },
};

enum ts_field_identifiers {
  field_kind = 1,
  field_lhs = 2,
  field_name = 3,
  field_op = 4,
  field_rhs = 5,
  field_value = 6,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_kind] = "kind",
  [field_lhs] = "lhs",
  [field_name] = "name",
  [field_op] = "op",
  [field_rhs] = "rhs",
  [field_value] = "value",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [1] = {.index = 0, .length = 1},
  [2] = {.index = 1, .length = 1},
  [4] = {.index = 2, .length = 2},
  [5] = {.index = 4, .length = 3},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_kind, 0},
  [1] =
    {field_name, 1},
  [2] =
    {field_name, 1},
    {field_value, 2},
  [4] =
    {field_lhs, 0},
    {field_op, 1},
    {field_rhs, 2},
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
  [3] = {
    [0] = sym_ident,
  },
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
  [12] = 12,
  [13] = 13,
  [14] = 14,
  [15] = 15,
  [16] = 16,
  [17] = 17,
  [18] = 18,
  [19] = 19,
  [20] = 20,
  [21] = 21,
  [22] = 22,
  [23] = 23,
  [24] = 24,
  [25] = 25,
  [26] = 26,
  [27] = 27,
  [28] = 28,
  [29] = 26,
  [30] = 25,
  [31] = 24,
  [32] = 32,
  [33] = 27,
  [34] = 28,
  [35] = 9,
  [36] = 8,
  [37] = 37,
  [38] = 38,
  [39] = 6,
  [40] = 11,
  [41] = 41,
  [42] = 12,
  [43] = 17,
  [44] = 19,
  [45] = 16,
  [46] = 46,
  [47] = 13,
  [48] = 14,
  [49] = 15,
  [50] = 50,
  [51] = 51,
  [52] = 52,
  [53] = 53,
  [54] = 54,
  [55] = 55,
  [56] = 56,
  [57] = 57,
  [58] = 58,
  [59] = 59,
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
  [94] = 94,
  [95] = 95,
  [96] = 96,
  [97] = 97,
  [98] = 98,
  [99] = 99,
  [100] = 100,
  [101] = 101,
  [102] = 102,
  [103] = 103,
  [104] = 104,
  [105] = 105,
  [106] = 106,
  [107] = 107,
  [108] = 108,
  [109] = 109,
  [110] = 110,
  [111] = 111,
  [112] = 112,
  [113] = 113,
  [114] = 114,
  [115] = 115,
  [116] = 116,
  [117] = 117,
  [118] = 118,
  [119] = 119,
  [120] = 120,
  [121] = 121,
  [122] = 122,
  [123] = 123,
  [124] = 124,
  [125] = 125,
  [126] = 126,
  [127] = 127,
  [128] = 128,
  [129] = 129,
  [130] = 130,
  [131] = 131,
  [132] = 132,
  [133] = 133,
  [134] = 125,
  [135] = 135,
  [136] = 136,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(30);
      ADVANCE_MAP(
        '\n', 31,
        '!', 58,
        '"', 1,
        '#', 65,
        '$', 19,
        '%', 60,
        '&', 64,
        '\'', 9,
        '(', 34,
        ')', 35,
        '*', 55,
        '+', 53,
        ',', 32,
        '-', 40,
        '.', 147,
        '/', 59,
        '0', 74,
        ':', 33,
        ';', 154,
        '=', 24,
        '[', 52,
        ']', 54,
        '^', 63,
        'b', 134,
        'c', 110,
        'd', 129,
        'l', 95,
        'p', 125,
        'q', 130,
        'r', 103,
        'w', 112,
        '{', 38,
        '|', 62,
        '}', 41,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(0);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(77);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 1:
      if (lookahead == '"') ADVANCE(93);
      if (lookahead != 0) ADVANCE(1);
      END_STATE();
    case 2:
      ADVANCE_MAP(
        '#', 65,
        '$', 20,
        '-', 6,
        '/', 10,
        '0', 84,
        ';', 154,
        'p', 16,
        'r', 13,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(2);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(86);
      END_STATE();
    case 3:
      if (lookahead == '#') ADVANCE(65);
      if (lookahead == '$') ADVANCE(21);
      if (lookahead == '-') ADVANCE(8);
      if (lookahead == '/') ADVANCE(10);
      if (lookahead == '0') ADVANCE(67);
      if (lookahead == ';') ADVANCE(154);
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(5);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(69);
      END_STATE();
    case 4:
      if (lookahead == '#') ADVANCE(65);
      if (lookahead == '%') ADVANCE(23);
      if (lookahead == '.') ADVANCE(147);
      if (lookahead == '/') ADVANCE(10);
      if (lookahead == ';') ADVANCE(154);
      if (lookahead == '$' ||
          lookahead == '=') ADVANCE(24);
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(4);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 5:
      if (lookahead == '#') ADVANCE(65);
      if (lookahead == '/') ADVANCE(10);
      if (lookahead == ';') ADVANCE(154);
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(5);
      END_STATE();
    case 6:
      if (lookahead == '$') ADVANCE(20);
      if (lookahead == '0') ADVANCE(84);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(86);
      END_STATE();
    case 7:
      if (lookahead == '$') ADVANCE(20);
      if (lookahead == '0') ADVANCE(82);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(83);
      END_STATE();
    case 8:
      if (lookahead == '$') ADVANCE(21);
      if (lookahead == '0') ADVANCE(67);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(69);
      END_STATE();
    case 9:
      if (lookahead == '\'') ADVANCE(94);
      if (lookahead != 0) ADVANCE(9);
      END_STATE();
    case 10:
      if (lookahead == '*') ADVANCE(12);
      if (lookahead == '/') ADVANCE(154);
      END_STATE();
    case 11:
      if (lookahead == '*') ADVANCE(11);
      if (lookahead == '/') ADVANCE(155);
      if (lookahead != 0) ADVANCE(12);
      END_STATE();
    case 12:
      if (lookahead == '*') ADVANCE(11);
      if (lookahead != 0) ADVANCE(12);
      END_STATE();
    case 13:
      if (lookahead == 'e') ADVANCE(14);
      END_STATE();
    case 14:
      if (lookahead == 'l') ADVANCE(56);
      END_STATE();
    case 15:
      if (lookahead == 'r') ADVANCE(50);
      END_STATE();
    case 16:
      if (lookahead == 't') ADVANCE(15);
      END_STATE();
    case 17:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(85);
      END_STATE();
    case 18:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(68);
      END_STATE();
    case 19:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(87);
      if (('G' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('g' <= lookahead && lookahead <= 'z')) ADVANCE(144);
      END_STATE();
    case 20:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(88);
      END_STATE();
    case 21:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(70);
      END_STATE();
    case 22:
      if (lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(146);
      END_STATE();
    case 23:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(143);
      END_STATE();
    case 24:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(144);
      END_STATE();
    case 25:
      if (eof) ADVANCE(30);
      ADVANCE_MAP(
        '\n', 31,
        '"', 1,
        '#', 65,
        '$', 19,
        '%', 23,
        '\'', 9,
        '(', 34,
        '*', 55,
        '-', 7,
        '.', 147,
        '/', 10,
        '0', 79,
        ':', 33,
        ';', 154,
        '=', 24,
        '[', 52,
        'b', 133,
        'd', 131,
        'q', 132,
        'w', 111,
        '{', 38,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(25);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(80);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      END_STATE();
    case 26:
      if (eof) ADVANCE(30);
      ADVANCE_MAP(
        '\n', 31,
        '"', 1,
        '#', 65,
        '$', 19,
        '%', 23,
        '\'', 9,
        '-', 7,
        '.', 147,
        '/', 10,
        '0', 79,
        ':', 33,
        ';', 154,
        '=', 24,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(26);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(80);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      END_STATE();
    case 27:
      if (eof) ADVANCE(30);
      ADVANCE_MAP(
        '\n', 31,
        '"', 1,
        '#', 65,
        '$', 19,
        '%', 61,
        '&', 64,
        '\'', 9,
        '(', 34,
        '*', 55,
        '+', 53,
        ',', 32,
        '-', 40,
        '.', 147,
        '/', 59,
        '0', 71,
        ';', 154,
        '=', 24,
        '^', 63,
        '|', 62,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(27);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(72);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      END_STATE();
    case 28:
      if (eof) ADVANCE(30);
      ADVANCE_MAP(
        '\n', 31,
        '#', 65,
        '%', 60,
        '&', 64,
        '(', 34,
        ')', 35,
        '*', 55,
        '+', 53,
        ',', 32,
        '-', 39,
        '.', 147,
        '/', 59,
        ';', 154,
        ']', 54,
        '^', 63,
        'c', 110,
        'l', 95,
        '|', 62,
        '}', 41,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(28);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 29:
      if (eof) ADVANCE(30);
      ADVANCE_MAP(
        '\n', 31,
        '#', 65,
        '%', 23,
        '(', 34,
        '.', 22,
        '/', 10,
        ';', 154,
        '}', 41,
        '$', 24,
        '=', 24,
      );
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(29);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(142);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(141);
      END_STATE();
    case 30:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 31:
      ACCEPT_TOKEN(anon_sym_LF);
      END_STATE();
    case 32:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 33:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 34:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 35:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 36:
      ACCEPT_TOKEN(anon_sym_label);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 37:
      ACCEPT_TOKEN(anon_sym_const);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 38:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      END_STATE();
    case 39:
      ACCEPT_TOKEN(anon_sym_DASH);
      END_STATE();
    case 40:
      ACCEPT_TOKEN(anon_sym_DASH);
      if (lookahead == '$') ADVANCE(20);
      if (lookahead == '0') ADVANCE(84);
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(86);
      END_STATE();
    case 41:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      END_STATE();
    case 42:
      ACCEPT_TOKEN(anon_sym_byte);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 43:
      ACCEPT_TOKEN(anon_sym_byte);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 44:
      ACCEPT_TOKEN(anon_sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 45:
      ACCEPT_TOKEN(anon_sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 46:
      ACCEPT_TOKEN(anon_sym_dword);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 47:
      ACCEPT_TOKEN(anon_sym_dword);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 48:
      ACCEPT_TOKEN(anon_sym_qword);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 49:
      ACCEPT_TOKEN(anon_sym_qword);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 50:
      ACCEPT_TOKEN(anon_sym_ptr);
      END_STATE();
    case 51:
      ACCEPT_TOKEN(anon_sym_ptr);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 52:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 53:
      ACCEPT_TOKEN(anon_sym_PLUS);
      END_STATE();
    case 54:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 55:
      ACCEPT_TOKEN(anon_sym_STAR);
      END_STATE();
    case 56:
      ACCEPT_TOKEN(anon_sym_rel);
      END_STATE();
    case 57:
      ACCEPT_TOKEN(anon_sym_rel);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 58:
      ACCEPT_TOKEN(anon_sym_BANG);
      END_STATE();
    case 59:
      ACCEPT_TOKEN(anon_sym_SLASH);
      if (lookahead == '*') ADVANCE(12);
      if (lookahead == '/') ADVANCE(154);
      END_STATE();
    case 60:
      ACCEPT_TOKEN(anon_sym_PERCENT);
      END_STATE();
    case 61:
      ACCEPT_TOKEN(anon_sym_PERCENT);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(143);
      END_STATE();
    case 62:
      ACCEPT_TOKEN(anon_sym_PIPE);
      END_STATE();
    case 63:
      ACCEPT_TOKEN(anon_sym_CARET);
      END_STATE();
    case 64:
      ACCEPT_TOKEN(anon_sym_AMP);
      END_STATE();
    case 65:
      ACCEPT_TOKEN(anon_sym_POUND);
      END_STATE();
    case 66:
      ACCEPT_TOKEN(anon_sym_POUND);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(153);
      END_STATE();
    case 67:
      ACCEPT_TOKEN(aux_sym_int_token1);
      if (lookahead == 'b') ADVANCE(18);
      if (lookahead == 'x') ADVANCE(21);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(69);
      END_STATE();
    case 68:
      ACCEPT_TOKEN(aux_sym_int_token1);
      if (lookahead == '0' ||
          lookahead == '1' ||
          lookahead == '_') ADVANCE(68);
      END_STATE();
    case 69:
      ACCEPT_TOKEN(aux_sym_int_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(69);
      END_STATE();
    case 70:
      ACCEPT_TOKEN(aux_sym_int_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(70);
      END_STATE();
    case 71:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == '_') ADVANCE(77);
      if (lookahead == 'b') ADVANCE(136);
      if (lookahead == 'x') ADVANCE(137);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(72);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      if (('A' <= lookahead && lookahead <= 'Z')) ADVANCE(140);
      END_STATE();
    case 72:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == '_') ADVANCE(77);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(72);
      if (('A' <= lookahead && lookahead <= 'Z')) ADVANCE(140);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 73:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == '_') ADVANCE(75);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(73);
      if (('A' <= lookahead && lookahead <= 'Z')) ADVANCE(140);
      if (('2' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 74:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'b') ADVANCE(135);
      if (lookahead == 'x') ADVANCE(138);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(77);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 75:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == '0' ||
          lookahead == '1' ||
          lookahead == '_') ADVANCE(75);
      if (('2' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 76:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'F') ||
          lookahead == '_') ADVANCE(78);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(76);
      if (('G' <= lookahead && lookahead <= 'Z')) ADVANCE(140);
      if (('g' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 77:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(77);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 78:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(78);
      if (('G' <= lookahead && lookahead <= 'Z') ||
          ('g' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 79:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(89);
      if (lookahead == '_') ADVANCE(81);
      if (lookahead == 'b') ADVANCE(136);
      if (lookahead == 'x') ADVANCE(137);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(80);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      if (('A' <= lookahead && lookahead <= 'Z')) ADVANCE(140);
      END_STATE();
    case 80:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(89);
      if (lookahead == '_') ADVANCE(81);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(80);
      if (('A' <= lookahead && lookahead <= 'Z')) ADVANCE(140);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 81:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(89);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(81);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 82:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(90);
      if (lookahead == 'b') ADVANCE(17);
      if (lookahead == 'x') ADVANCE(20);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(83);
      END_STATE();
    case 83:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '.') ADVANCE(90);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(83);
      END_STATE();
    case 84:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == 'b') ADVANCE(17);
      if (lookahead == 'x') ADVANCE(20);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(86);
      END_STATE();
    case 85:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (lookahead == '0' ||
          lookahead == '1' ||
          lookahead == '_') ADVANCE(85);
      END_STATE();
    case 86:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(86);
      END_STATE();
    case 87:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(87);
      if (('G' <= lookahead && lookahead <= 'Z') ||
          ('g' <= lookahead && lookahead <= 'z')) ADVANCE(144);
      END_STATE();
    case 88:
      ACCEPT_TOKEN(aux_sym_int_token2);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(88);
      END_STATE();
    case 89:
      ACCEPT_TOKEN(sym_float);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(91);
      if (lookahead == '.' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(148);
      END_STATE();
    case 90:
      ACCEPT_TOKEN(sym_float);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(92);
      END_STATE();
    case 91:
      ACCEPT_TOKEN(sym_float);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(91);
      if (lookahead == '.' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(148);
      END_STATE();
    case 92:
      ACCEPT_TOKEN(sym_float);
      if (('0' <= lookahead && lookahead <= '9') ||
          lookahead == '_') ADVANCE(92);
      END_STATE();
    case 93:
      ACCEPT_TOKEN(aux_sym_string_token1);
      END_STATE();
    case 94:
      ACCEPT_TOKEN(aux_sym_string_token2);
      END_STATE();
    case 95:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'a') ADVANCE(96);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 96:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'b') ADVANCE(106);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 97:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'd') ADVANCE(45);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 98:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'd') ADVANCE(47);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 99:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'd') ADVANCE(49);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 100:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'd') ADVANCE(44);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 101:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'd') ADVANCE(46);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 102:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'd') ADVANCE(48);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 103:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'e') ADVANCE(107);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 104:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'e') ADVANCE(43);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 105:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'e') ADVANCE(42);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 106:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'e') ADVANCE(108);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 107:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'l') ADVANCE(57);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 108:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'l') ADVANCE(36);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 109:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'n') ADVANCE(124);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 110:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(109);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 111:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(119);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 112:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(118);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 113:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(120);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 114:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(122);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 115:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(121);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 116:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'o') ADVANCE(123);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 117:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(51);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 118:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(97);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 119:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(100);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 120:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(98);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 121:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(101);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 122:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(99);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 123:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'r') ADVANCE(102);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 124:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 's') ADVANCE(126);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 125:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 't') ADVANCE(117);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 126:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 't') ADVANCE(37);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 127:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 't') ADVANCE(105);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 128:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 't') ADVANCE(104);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 129:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'w') ADVANCE(113);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 130:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'w') ADVANCE(114);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 131:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'w') ADVANCE(115);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 132:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'w') ADVANCE(116);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 133:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'y') ADVANCE(127);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 134:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == 'y') ADVANCE(128);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 135:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(75);
      if (('2' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 136:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(73);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('2' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 137:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'F')) ADVANCE(78);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(76);
      if (('g' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      if (('G' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      END_STATE();
    case 138:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(78);
      if (('G' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('g' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 139:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(140);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(139);
      END_STATE();
    case 140:
      ACCEPT_TOKEN(sym_word);
      if (lookahead == '.') ADVANCE(148);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(140);
      END_STATE();
    case 141:
      ACCEPT_TOKEN(sym_word);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_') ADVANCE(142);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(141);
      END_STATE();
    case 142:
      ACCEPT_TOKEN(sym_word);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(142);
      END_STATE();
    case 143:
      ACCEPT_TOKEN(sym__reg);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(143);
      END_STATE();
    case 144:
      ACCEPT_TOKEN(sym_address);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(144);
      END_STATE();
    case 145:
      ACCEPT_TOKEN(sym_meta_ident);
      if (lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(145);
      if (lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(148);
      END_STATE();
    case 146:
      ACCEPT_TOKEN(sym_meta_ident);
      if (lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(146);
      END_STATE();
    case 147:
      ACCEPT_TOKEN(sym__ident);
      if (lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(145);
      if (lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(148);
      END_STATE();
    case 148:
      ACCEPT_TOKEN(sym__ident);
      if (lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(148);
      END_STATE();
    case 149:
      ACCEPT_TOKEN(aux_sym_line_comment_token1);
      if (lookahead == '\n') ADVANCE(12);
      if (lookahead == '*') ADVANCE(149);
      if (lookahead == '/') ADVANCE(153);
      if (lookahead != 0) ADVANCE(150);
      END_STATE();
    case 150:
      ACCEPT_TOKEN(aux_sym_line_comment_token1);
      if (lookahead == '\n') ADVANCE(12);
      if (lookahead == '*') ADVANCE(149);
      if (lookahead != 0) ADVANCE(150);
      END_STATE();
    case 151:
      ACCEPT_TOKEN(aux_sym_line_comment_token1);
      if (lookahead == '#') ADVANCE(66);
      if (lookahead == '/') ADVANCE(152);
      if (lookahead == ';') ADVANCE(153);
      if (lookahead == '\t' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(151);
      if (lookahead != 0 &&
          lookahead != '\t' &&
          lookahead != '\n') ADVANCE(153);
      END_STATE();
    case 152:
      ACCEPT_TOKEN(aux_sym_line_comment_token1);
      if (lookahead == '*') ADVANCE(150);
      if (lookahead == '/') ADVANCE(153);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(153);
      END_STATE();
    case 153:
      ACCEPT_TOKEN(aux_sym_line_comment_token1);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(153);
      END_STATE();
    case 154:
      ACCEPT_TOKEN(aux_sym_line_comment_token2);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(154);
      END_STATE();
    case 155:
      ACCEPT_TOKEN(sym_block_comment);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 28},
  [2] = {.lex_state = 25},
  [3] = {.lex_state = 25},
  [4] = {.lex_state = 25},
  [5] = {.lex_state = 25},
  [6] = {.lex_state = 27},
  [7] = {.lex_state = 27},
  [8] = {.lex_state = 27},
  [9] = {.lex_state = 27},
  [10] = {.lex_state = 27},
  [11] = {.lex_state = 27},
  [12] = {.lex_state = 27},
  [13] = {.lex_state = 27},
  [14] = {.lex_state = 27},
  [15] = {.lex_state = 27},
  [16] = {.lex_state = 27},
  [17] = {.lex_state = 27},
  [18] = {.lex_state = 27},
  [19] = {.lex_state = 27},
  [20] = {.lex_state = 27},
  [21] = {.lex_state = 27},
  [22] = {.lex_state = 26},
  [23] = {.lex_state = 26},
  [24] = {.lex_state = 27},
  [25] = {.lex_state = 27},
  [26] = {.lex_state = 27},
  [27] = {.lex_state = 27},
  [28] = {.lex_state = 27},
  [29] = {.lex_state = 27},
  [30] = {.lex_state = 27},
  [31] = {.lex_state = 27},
  [32] = {.lex_state = 27},
  [33] = {.lex_state = 27},
  [34] = {.lex_state = 27},
  [35] = {.lex_state = 28},
  [36] = {.lex_state = 28},
  [37] = {.lex_state = 28},
  [38] = {.lex_state = 28},
  [39] = {.lex_state = 28},
  [40] = {.lex_state = 28},
  [41] = {.lex_state = 28},
  [42] = {.lex_state = 28},
  [43] = {.lex_state = 28},
  [44] = {.lex_state = 28},
  [45] = {.lex_state = 28},
  [46] = {.lex_state = 28},
  [47] = {.lex_state = 28},
  [48] = {.lex_state = 28},
  [49] = {.lex_state = 28},
  [50] = {.lex_state = 27},
  [51] = {.lex_state = 27},
  [52] = {.lex_state = 28},
  [53] = {.lex_state = 28},
  [54] = {.lex_state = 4},
  [55] = {.lex_state = 4},
  [56] = {.lex_state = 29},
  [57] = {.lex_state = 29},
  [58] = {.lex_state = 29},
  [59] = {.lex_state = 29},
  [60] = {.lex_state = 29},
  [61] = {.lex_state = 0},
  [62] = {.lex_state = 28},
  [63] = {.lex_state = 28},
  [64] = {.lex_state = 0},
  [65] = {.lex_state = 0},
  [66] = {.lex_state = 0},
  [67] = {.lex_state = 29},
  [68] = {.lex_state = 0},
  [69] = {.lex_state = 0},
  [70] = {.lex_state = 0},
  [71] = {.lex_state = 0},
  [72] = {.lex_state = 29},
  [73] = {.lex_state = 29},
  [74] = {.lex_state = 0},
  [75] = {.lex_state = 29},
  [76] = {.lex_state = 28},
  [77] = {.lex_state = 0},
  [78] = {.lex_state = 0},
  [79] = {.lex_state = 29},
  [80] = {.lex_state = 0},
  [81] = {.lex_state = 0},
  [82] = {.lex_state = 28},
  [83] = {.lex_state = 0},
  [84] = {.lex_state = 0},
  [85] = {.lex_state = 0},
  [86] = {.lex_state = 0},
  [87] = {.lex_state = 0},
  [88] = {.lex_state = 0},
  [89] = {.lex_state = 0},
  [90] = {.lex_state = 0},
  [91] = {.lex_state = 0},
  [92] = {.lex_state = 0},
  [93] = {.lex_state = 0},
  [94] = {.lex_state = 0},
  [95] = {.lex_state = 0},
  [96] = {.lex_state = 0},
  [97] = {.lex_state = 0},
  [98] = {.lex_state = 0},
  [99] = {.lex_state = 0},
  [100] = {.lex_state = 0},
  [101] = {.lex_state = 0},
  [102] = {.lex_state = 28},
  [103] = {.lex_state = 0},
  [104] = {.lex_state = 28},
  [105] = {.lex_state = 2},
  [106] = {.lex_state = 0},
  [107] = {.lex_state = 0},
  [108] = {.lex_state = 0},
  [109] = {.lex_state = 2},
  [110] = {.lex_state = 0},
  [111] = {.lex_state = 0},
  [112] = {.lex_state = 0},
  [113] = {.lex_state = 0},
  [114] = {.lex_state = 0},
  [115] = {.lex_state = 2},
  [116] = {.lex_state = 151},
  [117] = {.lex_state = 0},
  [118] = {.lex_state = 0},
  [119] = {.lex_state = 0},
  [120] = {.lex_state = 0},
  [121] = {.lex_state = 0},
  [122] = {.lex_state = 0},
  [123] = {.lex_state = 0},
  [124] = {.lex_state = 25},
  [125] = {.lex_state = 3},
  [126] = {.lex_state = 2},
  [127] = {.lex_state = 2},
  [128] = {.lex_state = 0},
  [129] = {.lex_state = 0},
  [130] = {.lex_state = 0},
  [131] = {.lex_state = 0},
  [132] = {.lex_state = 29},
  [133] = {.lex_state = 29},
  [134] = {.lex_state = 3},
  [135] = {(TSStateId)(-1)},
  [136] = {(TSStateId)(-1)},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [sym_line_comment] = STATE(0),
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_LF] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [anon_sym_COLON] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [anon_sym_label] = ACTIONS(1),
    [anon_sym_const] = ACTIONS(1),
    [anon_sym_LBRACE] = ACTIONS(1),
    [anon_sym_DASH] = ACTIONS(1),
    [anon_sym_RBRACE] = ACTIONS(1),
    [anon_sym_byte] = ACTIONS(1),
    [anon_sym_word] = ACTIONS(1),
    [anon_sym_dword] = ACTIONS(1),
    [anon_sym_qword] = ACTIONS(1),
    [anon_sym_ptr] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_PLUS] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_STAR] = ACTIONS(1),
    [anon_sym_rel] = ACTIONS(1),
    [anon_sym_BANG] = ACTIONS(1),
    [anon_sym_SLASH] = ACTIONS(1),
    [anon_sym_PERCENT] = ACTIONS(1),
    [anon_sym_PIPE] = ACTIONS(1),
    [anon_sym_CARET] = ACTIONS(1),
    [anon_sym_AMP] = ACTIONS(1),
    [anon_sym_POUND] = ACTIONS(3),
    [aux_sym_int_token2] = ACTIONS(1),
    [aux_sym_string_token1] = ACTIONS(1),
    [aux_sym_string_token2] = ACTIONS(1),
    [sym_word] = ACTIONS(1),
    [sym_address] = ACTIONS(1),
    [sym_meta_ident] = ACTIONS(1),
    [sym__ident] = ACTIONS(1),
    [aux_sym_line_comment_token2] = ACTIONS(5),
    [sym_block_comment] = ACTIONS(7),
  },
  [1] = {
    [sym_program] = STATE(130),
    [sym__item] = STATE(81),
    [sym_meta] = STATE(114),
    [sym_label] = STATE(114),
    [sym_const] = STATE(114),
    [sym_instruction] = STATE(114),
    [sym_line_comment] = STATE(1),
    [ts_builtin_sym_end] = ACTIONS(9),
    [anon_sym_label] = ACTIONS(11),
    [anon_sym_const] = ACTIONS(13),
    [anon_sym_POUND] = ACTIONS(3),
    [sym_word] = ACTIONS(15),
    [sym_meta_ident] = ACTIONS(17),
    [sym__ident] = ACTIONS(19),
    [aux_sym_line_comment_token2] = ACTIONS(5),
    [sym_block_comment] = ACTIONS(7),
  },
  [2] = {
    [sym__expr] = STATE(69),
    [sym_list] = STATE(100),
    [sym_ptr] = STATE(100),
    [sym__tc_expr] = STATE(18),
    [sym_tc_infix] = STATE(17),
    [sym_int] = STATE(7),
    [sym_string] = STATE(10),
    [sym_reg] = STATE(11),
    [sym_ident] = STATE(10),
    [sym_line_comment] = STATE(2),
    [aux_sym_instruction_repeat2] = STATE(20),
    [ts_builtin_sym_end] = ACTIONS(21),
    [anon_sym_LF] = ACTIONS(21),
    [anon_sym_COLON] = ACTIONS(23),
    [anon_sym_LPAREN] = ACTIONS(25),
    [anon_sym_LBRACE] = ACTIONS(27),
    [anon_sym_byte] = ACTIONS(29),
    [anon_sym_word] = ACTIONS(29),
    [anon_sym_dword] = ACTIONS(29),
    [anon_sym_qword] = ACTIONS(29),
    [anon_sym_LBRACK] = ACTIONS(31),
    [anon_sym_STAR] = ACTIONS(33),
    [anon_sym_POUND] = ACTIONS(35),
    [aux_sym_int_token2] = ACTIONS(37),
    [sym_float] = ACTIONS(39),
    [aux_sym_string_token1] = ACTIONS(41),
    [aux_sym_string_token2] = ACTIONS(41),
    [sym_word] = ACTIONS(43),
    [sym__reg] = ACTIONS(43),
    [sym_address] = ACTIONS(43),
    [sym_meta_ident] = ACTIONS(45),
    [sym__ident] = ACTIONS(45),
    [aux_sym_line_comment_token2] = ACTIONS(5),
    [sym_block_comment] = ACTIONS(7),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 19,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(25), 1,
      anon_sym_LPAREN,
    ACTIONS(27), 1,
      anon_sym_LBRACE,
    ACTIONS(31), 1,
      anon_sym_LBRACK,
    ACTIONS(33), 1,
      anon_sym_STAR,
    ACTIONS(39), 1,
      sym_float,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(3), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(78), 1,
      sym_int,
    STATE(91), 1,
      sym__expr,
    ACTIONS(47), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    ACTIONS(29), 4,
      anon_sym_byte,
      anon_sym_word,
      anon_sym_dword,
      anon_sym_qword,
    STATE(100), 4,
      sym_list,
      sym_ptr,
      sym_string,
      sym_ident,
  [69] = 19,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(25), 1,
      anon_sym_LPAREN,
    ACTIONS(27), 1,
      anon_sym_LBRACE,
    ACTIONS(31), 1,
      anon_sym_LBRACK,
    ACTIONS(33), 1,
      anon_sym_STAR,
    ACTIONS(39), 1,
      sym_float,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(4), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(78), 1,
      sym_int,
    STATE(91), 1,
      sym__expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(59), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    ACTIONS(29), 4,
      anon_sym_byte,
      anon_sym_word,
      anon_sym_dword,
      anon_sym_qword,
    STATE(100), 4,
      sym_list,
      sym_ptr,
      sym_string,
      sym_ident,
  [138] = 18,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(25), 1,
      anon_sym_LPAREN,
    ACTIONS(27), 1,
      anon_sym_LBRACE,
    ACTIONS(31), 1,
      anon_sym_LBRACK,
    ACTIONS(33), 1,
      anon_sym_STAR,
    ACTIONS(39), 1,
      sym_float,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(5), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(78), 1,
      sym_int,
    STATE(91), 1,
      sym__expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    ACTIONS(29), 4,
      anon_sym_byte,
      anon_sym_word,
      anon_sym_dword,
      anon_sym_qword,
    STATE(100), 4,
      sym_list,
      sym_ptr,
      sym_string,
      sym_ident,
  [203] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(6), 1,
      sym_line_comment,
    ACTIONS(63), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(61), 12,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_LPAREN,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [238] = 8,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(68), 1,
      anon_sym_COMMA,
    ACTIONS(70), 1,
      anon_sym_LPAREN,
    STATE(7), 1,
      sym_line_comment,
    ACTIONS(65), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(74), 8,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(72), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [279] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(8), 1,
      sym_line_comment,
    ACTIONS(78), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(76), 12,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_LPAREN,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [314] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(9), 1,
      sym_line_comment,
    ACTIONS(82), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(80), 11,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [348] = 7,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(68), 1,
      anon_sym_COMMA,
    STATE(10), 1,
      sym_line_comment,
    ACTIONS(65), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(74), 8,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(72), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [386] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(11), 1,
      sym_line_comment,
    ACTIONS(86), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(84), 11,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [420] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(12), 1,
      sym_line_comment,
    ACTIONS(90), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(88), 11,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [454] = 8,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(96), 1,
      anon_sym_PIPE,
    ACTIONS(98), 1,
      anon_sym_CARET,
    ACTIONS(100), 1,
      anon_sym_AMP,
    STATE(13), 1,
      sym_line_comment,
    ACTIONS(92), 7,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(94), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [493] = 10,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(96), 1,
      anon_sym_PIPE,
    ACTIONS(98), 1,
      anon_sym_CARET,
    ACTIONS(100), 1,
      anon_sym_AMP,
    ACTIONS(102), 1,
      anon_sym_STAR,
    STATE(14), 1,
      sym_line_comment,
    ACTIONS(104), 2,
      anon_sym_SLASH,
      anon_sym_PERCENT,
    ACTIONS(92), 6,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_PLUS,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(94), 7,
      anon_sym_DASH,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [536] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(15), 1,
      sym_line_comment,
    ACTIONS(94), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(92), 10,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [569] = 7,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(98), 1,
      anon_sym_CARET,
    ACTIONS(100), 1,
      anon_sym_AMP,
    STATE(16), 1,
      sym_line_comment,
    ACTIONS(92), 8,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(94), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [606] = 5,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(17), 1,
      sym_line_comment,
    ACTIONS(72), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
    ACTIONS(74), 10,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [639] = 12,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(96), 1,
      anon_sym_PIPE,
    ACTIONS(98), 1,
      anon_sym_CARET,
    ACTIONS(100), 1,
      anon_sym_AMP,
    ACTIONS(102), 1,
      anon_sym_STAR,
    ACTIONS(108), 1,
      anon_sym_DASH,
    ACTIONS(110), 1,
      anon_sym_PLUS,
    STATE(18), 1,
      sym_line_comment,
    ACTIONS(104), 2,
      anon_sym_SLASH,
      anon_sym_PERCENT,
    ACTIONS(106), 5,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(112), 6,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [686] = 6,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(100), 1,
      anon_sym_AMP,
    STATE(19), 1,
      sym_line_comment,
    ACTIONS(92), 9,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_POUND,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(94), 9,
      anon_sym_DASH,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      aux_sym_int_token2,
      sym_word,
      sym__reg,
      sym_address,
      sym_meta_ident,
      sym__ident,
  [721] = 13,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(35), 1,
      anon_sym_POUND,
    ACTIONS(37), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(18), 1,
      sym__tc_expr,
    STATE(20), 1,
      sym_line_comment,
    STATE(21), 1,
      aux_sym_instruction_repeat2,
    ACTIONS(41), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(45), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(114), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(43), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [769] = 12,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(118), 1,
      anon_sym_POUND,
    ACTIONS(121), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(18), 1,
      sym__tc_expr,
    ACTIONS(116), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(124), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(130), 2,
      sym_meta_ident,
      sym__ident,
    STATE(21), 2,
      sym_line_comment,
      aux_sym_instruction_repeat2,
    ACTIONS(127), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [815] = 15,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    ACTIONS(135), 1,
      anon_sym_COLON,
    ACTIONS(137), 1,
      sym_float,
    STATE(22), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(65), 1,
      sym_string,
    STATE(74), 1,
      sym_int,
    STATE(106), 1,
      sym_ident,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(133), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
  [866] = 14,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    ACTIONS(137), 1,
      sym_float,
    STATE(23), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(65), 1,
      sym_string,
    STATE(74), 1,
      sym_int,
    STATE(106), 1,
      sym_ident,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(133), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
  [914] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(24), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(49), 1,
      sym__tc_expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(43), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [955] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(35), 1,
      anon_sym_POUND,
    ACTIONS(37), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(14), 1,
      sym__tc_expr,
    STATE(25), 1,
      sym_line_comment,
    ACTIONS(41), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(45), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(43), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [996] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(26), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(44), 1,
      sym__tc_expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(43), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1037] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(35), 1,
      anon_sym_POUND,
    ACTIONS(37), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(13), 1,
      sym__tc_expr,
    STATE(27), 1,
      sym_line_comment,
    ACTIONS(41), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(45), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(43), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1078] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(35), 1,
      anon_sym_POUND,
    ACTIONS(37), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(16), 1,
      sym__tc_expr,
    STATE(28), 1,
      sym_line_comment,
    ACTIONS(41), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(45), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(43), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1119] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(35), 1,
      anon_sym_POUND,
    ACTIONS(37), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(19), 1,
      sym__tc_expr,
    STATE(29), 1,
      sym_line_comment,
    ACTIONS(41), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(45), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(43), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1160] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(30), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(48), 1,
      sym__tc_expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(43), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1201] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(35), 1,
      anon_sym_POUND,
    ACTIONS(37), 1,
      aux_sym_int_token2,
    STATE(11), 1,
      sym_reg,
    STATE(15), 1,
      sym__tc_expr,
    STATE(31), 1,
      sym_line_comment,
    ACTIONS(41), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(45), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(43), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(17), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1242] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(32), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(46), 1,
      sym__tc_expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(43), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1283] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(33), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(47), 1,
      sym__tc_expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(43), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1324] = 11,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(34), 1,
      sym_line_comment,
    STATE(40), 1,
      sym_reg,
    STATE(45), 1,
      sym__tc_expr,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
    STATE(43), 4,
      sym_tc_infix,
      sym_int,
      sym_string,
      sym_ident,
  [1365] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(82), 1,
      anon_sym_SLASH,
    STATE(35), 1,
      sym_line_comment,
    ACTIONS(80), 13,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_DASH,
      anon_sym_RBRACE,
      anon_sym_PLUS,
      anon_sym_RBRACK,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1396] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(78), 1,
      anon_sym_SLASH,
    STATE(36), 1,
      sym_line_comment,
    ACTIONS(76), 12,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_LPAREN,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_RBRACK,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1426] = 14,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(11), 1,
      anon_sym_label,
    ACTIONS(13), 1,
      anon_sym_const,
    ACTIONS(15), 1,
      sym_word,
    ACTIONS(17), 1,
      sym_meta_ident,
    ACTIONS(19), 1,
      sym__ident,
    ACTIONS(139), 1,
      ts_builtin_sym_end,
    ACTIONS(141), 1,
      anon_sym_LF,
    STATE(37), 1,
      sym_line_comment,
    STATE(52), 1,
      aux_sym_program_repeat1,
    STATE(110), 1,
      sym__item,
    STATE(114), 4,
      sym_meta,
      sym_label,
      sym_const,
      sym_instruction,
  [1472] = 14,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(11), 1,
      anon_sym_label,
    ACTIONS(13), 1,
      anon_sym_const,
    ACTIONS(15), 1,
      sym_word,
    ACTIONS(17), 1,
      sym_meta_ident,
    ACTIONS(19), 1,
      sym__ident,
    ACTIONS(141), 1,
      anon_sym_LF,
    ACTIONS(143), 1,
      ts_builtin_sym_end,
    STATE(38), 1,
      sym_line_comment,
    STATE(52), 1,
      aux_sym_program_repeat1,
    STATE(110), 1,
      sym__item,
    STATE(114), 4,
      sym_meta,
      sym_label,
      sym_const,
      sym_instruction,
  [1518] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(63), 1,
      anon_sym_SLASH,
    STATE(39), 1,
      sym_line_comment,
    ACTIONS(61), 12,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_LPAREN,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_RBRACK,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1548] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(86), 1,
      anon_sym_SLASH,
    STATE(40), 1,
      sym_line_comment,
    ACTIONS(84), 12,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_RBRACK,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1578] = 13,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(11), 1,
      anon_sym_label,
    ACTIONS(13), 1,
      anon_sym_const,
    ACTIONS(15), 1,
      sym_word,
    ACTIONS(17), 1,
      sym_meta_ident,
    ACTIONS(19), 1,
      sym__ident,
    ACTIONS(141), 1,
      anon_sym_LF,
    STATE(41), 1,
      sym_line_comment,
    STATE(52), 1,
      aux_sym_program_repeat1,
    STATE(110), 1,
      sym__item,
    STATE(114), 4,
      sym_meta,
      sym_label,
      sym_const,
      sym_instruction,
  [1621] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(90), 1,
      anon_sym_SLASH,
    STATE(42), 1,
      sym_line_comment,
    ACTIONS(88), 10,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1649] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(72), 1,
      anon_sym_SLASH,
    STATE(43), 1,
      sym_line_comment,
    ACTIONS(74), 9,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1676] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(94), 1,
      anon_sym_SLASH,
    ACTIONS(145), 1,
      anon_sym_AMP,
    STATE(44), 1,
      sym_line_comment,
    ACTIONS(92), 8,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
  [1705] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(94), 1,
      anon_sym_SLASH,
    ACTIONS(145), 1,
      anon_sym_AMP,
    ACTIONS(147), 1,
      anon_sym_CARET,
    STATE(45), 1,
      sym_line_comment,
    ACTIONS(92), 7,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
  [1736] = 11,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(145), 1,
      anon_sym_AMP,
    ACTIONS(147), 1,
      anon_sym_CARET,
    ACTIONS(155), 1,
      anon_sym_SLASH,
    ACTIONS(157), 1,
      anon_sym_PIPE,
    STATE(46), 1,
      sym_line_comment,
    ACTIONS(149), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(151), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
    ACTIONS(153), 2,
      anon_sym_STAR,
      anon_sym_PERCENT,
  [1773] = 9,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(94), 1,
      anon_sym_SLASH,
    ACTIONS(145), 1,
      anon_sym_AMP,
    ACTIONS(147), 1,
      anon_sym_CARET,
    ACTIONS(157), 1,
      anon_sym_PIPE,
    STATE(47), 1,
      sym_line_comment,
    ACTIONS(92), 6,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PERCENT,
  [1806] = 10,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(145), 1,
      anon_sym_AMP,
    ACTIONS(147), 1,
      anon_sym_CARET,
    ACTIONS(155), 1,
      anon_sym_SLASH,
    ACTIONS(157), 1,
      anon_sym_PIPE,
    STATE(48), 1,
      sym_line_comment,
    ACTIONS(153), 2,
      anon_sym_STAR,
      anon_sym_PERCENT,
    ACTIONS(92), 4,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_DASH,
      anon_sym_PLUS,
  [1841] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(94), 1,
      anon_sym_SLASH,
    STATE(49), 1,
      sym_line_comment,
    ACTIONS(92), 9,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_DASH,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_PERCENT,
      anon_sym_PIPE,
      anon_sym_CARET,
      anon_sym_AMP,
  [1868] = 9,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(40), 1,
      sym_reg,
    STATE(50), 1,
      sym_line_comment,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    STATE(122), 2,
      sym_int,
      sym_ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
  [1900] = 9,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(51), 1,
      aux_sym_int_token2,
    STATE(40), 1,
      sym_reg,
    STATE(51), 1,
      sym_line_comment,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
    STATE(117), 2,
      sym_int,
      sym_ident,
    ACTIONS(55), 3,
      sym_word,
      sym__reg,
      sym_address,
  [1932] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(159), 1,
      ts_builtin_sym_end,
    ACTIONS(161), 1,
      anon_sym_LF,
    STATE(52), 2,
      sym_line_comment,
      aux_sym_program_repeat1,
    ACTIONS(164), 5,
      anon_sym_label,
      anon_sym_const,
      sym_word,
      sym_meta_ident,
      sym__ident,
  [1959] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(53), 1,
      sym_line_comment,
    ACTIONS(166), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    ACTIONS(168), 5,
      anon_sym_label,
      anon_sym_const,
      sym_word,
      sym_meta_ident,
      sym__ident,
  [1983] = 9,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(170), 1,
      sym_address,
    STATE(40), 1,
      sym_reg,
    STATE(54), 1,
      sym_line_comment,
    STATE(128), 1,
      sym_ident,
    ACTIONS(55), 2,
      sym_word,
      sym__reg,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
  [2013] = 9,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(170), 1,
      sym_address,
    STATE(40), 1,
      sym_reg,
    STATE(55), 1,
      sym_line_comment,
    STATE(129), 1,
      sym_ident,
    ACTIONS(55), 2,
      sym_word,
      sym__reg,
    ACTIONS(57), 2,
      sym_meta_ident,
      sym__ident,
  [2043] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    ACTIONS(172), 1,
      anon_sym_RBRACE,
    STATE(56), 1,
      sym_line_comment,
    STATE(82), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2069] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    ACTIONS(174), 1,
      anon_sym_RBRACE,
    STATE(57), 1,
      sym_line_comment,
    STATE(104), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2095] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(178), 1,
      anon_sym_LPAREN,
    ACTIONS(180), 1,
      sym_meta_ident,
    STATE(58), 1,
      sym_line_comment,
    STATE(108), 1,
      sym_meta,
    ACTIONS(176), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2121] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    ACTIONS(182), 1,
      anon_sym_RBRACE,
    STATE(59), 1,
      sym_line_comment,
    STATE(104), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2147] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(180), 1,
      sym_meta_ident,
    ACTIONS(186), 1,
      anon_sym_LPAREN,
    STATE(60), 1,
      sym_line_comment,
    STATE(111), 1,
      sym_meta,
    ACTIONS(184), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2173] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(188), 1,
      ts_builtin_sym_end,
    ACTIONS(190), 1,
      anon_sym_LF,
    STATE(41), 1,
      aux_sym_program_repeat1,
    STATE(61), 2,
      sym_line_comment,
      aux_sym_program_repeat2,
  [2196] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(182), 1,
      anon_sym_RBRACE,
    ACTIONS(193), 1,
      anon_sym_COMMA,
    ACTIONS(195), 1,
      anon_sym_DASH,
    STATE(62), 1,
      sym_line_comment,
    STATE(63), 1,
      aux_sym_list_repeat1,
  [2221] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(200), 1,
      anon_sym_RBRACE,
    ACTIONS(197), 2,
      anon_sym_COMMA,
      anon_sym_DASH,
    STATE(63), 2,
      sym_line_comment,
      aux_sym_list_repeat1,
  [2242] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(204), 1,
      anon_sym_COMMA,
    STATE(64), 1,
      sym_line_comment,
    STATE(86), 1,
      aux_sym_meta_repeat2,
    ACTIONS(202), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2265] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(208), 1,
      anon_sym_COMMA,
    STATE(65), 1,
      sym_line_comment,
    STATE(68), 1,
      aux_sym_meta_repeat3,
    ACTIONS(206), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2288] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(210), 1,
      anon_sym_COMMA,
    STATE(66), 1,
      sym_line_comment,
    STATE(87), 1,
      aux_sym_meta_repeat1,
    ACTIONS(202), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2311] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    STATE(67), 1,
      sym_line_comment,
    STATE(121), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2334] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(208), 1,
      anon_sym_COMMA,
    STATE(68), 1,
      sym_line_comment,
    STATE(84), 1,
      aux_sym_meta_repeat3,
    ACTIONS(202), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2357] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(212), 1,
      anon_sym_COMMA,
    STATE(69), 1,
      sym_line_comment,
    STATE(71), 1,
      aux_sym_instruction_repeat1,
    ACTIONS(114), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2380] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(204), 1,
      anon_sym_COMMA,
    STATE(64), 1,
      aux_sym_meta_repeat2,
    STATE(70), 1,
      sym_line_comment,
    ACTIONS(206), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2403] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(214), 1,
      anon_sym_COMMA,
    STATE(71), 1,
      sym_line_comment,
    STATE(80), 1,
      aux_sym_instruction_repeat1,
    ACTIONS(47), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2426] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    STATE(72), 1,
      sym_line_comment,
    STATE(120), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2449] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    STATE(73), 1,
      sym_line_comment,
    STATE(104), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2472] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(210), 1,
      anon_sym_COMMA,
    STATE(66), 1,
      aux_sym_meta_repeat1,
    STATE(74), 1,
      sym_line_comment,
    ACTIONS(206), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [2495] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    STATE(75), 1,
      sym_line_comment,
    STATE(102), 1,
      sym_reg,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2518] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(216), 1,
      anon_sym_COMMA,
    ACTIONS(220), 1,
      anon_sym_RBRACK,
    STATE(76), 1,
      sym_line_comment,
    ACTIONS(218), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
  [2541] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(224), 1,
      anon_sym_BANG,
    STATE(77), 1,
      sym_line_comment,
    ACTIONS(222), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2562] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(70), 1,
      anon_sym_LPAREN,
    STATE(78), 1,
      sym_line_comment,
    ACTIONS(68), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2583] = 7,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(55), 1,
      sym__reg,
    STATE(76), 1,
      sym_reg,
    STATE(79), 1,
      sym_line_comment,
    ACTIONS(170), 2,
      sym_word,
      sym_address,
  [2606] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(228), 1,
      anon_sym_COMMA,
    ACTIONS(226), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    STATE(80), 2,
      sym_line_comment,
      aux_sym_instruction_repeat1,
  [2627] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(141), 1,
      anon_sym_LF,
    ACTIONS(231), 1,
      ts_builtin_sym_end,
    STATE(37), 1,
      aux_sym_program_repeat1,
    STATE(81), 1,
      sym_line_comment,
    STATE(85), 1,
      aux_sym_program_repeat2,
  [2652] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(195), 1,
      anon_sym_DASH,
    ACTIONS(233), 1,
      anon_sym_COMMA,
    ACTIONS(235), 1,
      anon_sym_RBRACE,
    STATE(62), 1,
      aux_sym_list_repeat1,
    STATE(82), 1,
      sym_line_comment,
  [2677] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(239), 1,
      anon_sym_BANG,
    STATE(83), 1,
      sym_line_comment,
    ACTIONS(237), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2698] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(243), 1,
      anon_sym_COMMA,
    ACTIONS(241), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    STATE(84), 2,
      sym_line_comment,
      aux_sym_meta_repeat3,
  [2719] = 8,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(139), 1,
      ts_builtin_sym_end,
    ACTIONS(141), 1,
      anon_sym_LF,
    STATE(38), 1,
      aux_sym_program_repeat1,
    STATE(61), 1,
      aux_sym_program_repeat2,
    STATE(85), 1,
      sym_line_comment,
  [2744] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(248), 1,
      anon_sym_COMMA,
    ACTIONS(246), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    STATE(86), 2,
      sym_line_comment,
      aux_sym_meta_repeat2,
  [2765] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(253), 1,
      anon_sym_COMMA,
    ACTIONS(251), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
    STATE(87), 2,
      sym_line_comment,
      aux_sym_meta_repeat1,
  [2786] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(88), 1,
      sym_line_comment,
    STATE(99), 1,
      sym_string,
    ACTIONS(53), 2,
      aux_sym_string_token1,
      aux_sym_string_token2,
  [2806] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(89), 1,
      sym_line_comment,
    ACTIONS(256), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2824] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(90), 1,
      sym_line_comment,
    ACTIONS(258), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2842] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(91), 1,
      sym_line_comment,
    ACTIONS(226), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2860] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(92), 1,
      sym_line_comment,
    ACTIONS(222), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2878] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(93), 1,
      sym_line_comment,
    ACTIONS(237), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2896] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(94), 1,
      sym_line_comment,
    ACTIONS(260), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2914] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(95), 1,
      sym_line_comment,
    ACTIONS(246), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2932] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(96), 1,
      sym_line_comment,
    ACTIONS(262), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2950] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(97), 1,
      sym_line_comment,
    ACTIONS(251), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2968] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(98), 1,
      sym_line_comment,
    ACTIONS(264), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [2986] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(99), 1,
      sym_line_comment,
    ACTIONS(241), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [3004] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(100), 1,
      sym_line_comment,
    ACTIONS(68), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [3022] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(101), 1,
      sym_line_comment,
    ACTIONS(266), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [3040] = 6,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(270), 1,
      anon_sym_RBRACK,
    STATE(102), 1,
      sym_line_comment,
    ACTIONS(268), 2,
      anon_sym_DASH,
      anon_sym_PLUS,
  [3060] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(103), 1,
      sym_line_comment,
    ACTIONS(272), 3,
      ts_builtin_sym_end,
      anon_sym_LF,
      anon_sym_COMMA,
  [3078] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(104), 1,
      sym_line_comment,
    ACTIONS(200), 3,
      anon_sym_COMMA,
      anon_sym_DASH,
      anon_sym_RBRACE,
  [3096] = 6,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(274), 1,
      aux_sym_int_token2,
    STATE(97), 1,
      sym_int,
    STATE(105), 1,
      sym_line_comment,
  [3115] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(106), 1,
      sym_line_comment,
    ACTIONS(206), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3132] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(107), 1,
      sym_line_comment,
    ACTIONS(276), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3149] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(108), 1,
      sym_line_comment,
    ACTIONS(278), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3166] = 6,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(274), 1,
      aux_sym_int_token2,
    STATE(109), 1,
      sym_line_comment,
    STATE(123), 1,
      sym_int,
  [3185] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(110), 1,
      sym_line_comment,
    ACTIONS(188), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3202] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(111), 1,
      sym_line_comment,
    ACTIONS(280), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3219] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(112), 1,
      sym_line_comment,
    ACTIONS(282), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3236] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(113), 1,
      sym_line_comment,
    ACTIONS(284), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3253] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    STATE(114), 1,
      sym_line_comment,
    ACTIONS(286), 2,
      ts_builtin_sym_end,
      anon_sym_LF,
  [3270] = 6,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(49), 1,
      anon_sym_POUND,
    ACTIONS(274), 1,
      aux_sym_int_token2,
    STATE(115), 1,
      sym_line_comment,
    STATE(122), 1,
      sym_int,
  [3289] = 5,
    ACTIONS(288), 1,
      anon_sym_POUND,
    ACTIONS(290), 1,
      aux_sym_line_comment_token1,
    ACTIONS(292), 1,
      aux_sym_line_comment_token2,
    ACTIONS(294), 1,
      sym_block_comment,
    STATE(116), 1,
      sym_line_comment,
  [3305] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(296), 1,
      anon_sym_RBRACK,
    STATE(117), 1,
      sym_line_comment,
  [3321] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(298), 1,
      anon_sym_LBRACK,
    STATE(118), 1,
      sym_line_comment,
  [3337] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(300), 1,
      anon_sym_LBRACK,
    STATE(119), 1,
      sym_line_comment,
  [3353] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(224), 1,
      anon_sym_RPAREN,
    STATE(120), 1,
      sym_line_comment,
  [3369] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(302), 1,
      anon_sym_RPAREN,
    STATE(121), 1,
      sym_line_comment,
  [3385] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(270), 1,
      anon_sym_RBRACK,
    STATE(122), 1,
      sym_line_comment,
  [3401] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(304), 1,
      anon_sym_RBRACK,
    STATE(123), 1,
      sym_line_comment,
  [3417] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(306), 1,
      sym_float,
    STATE(124), 1,
      sym_line_comment,
  [3433] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(308), 1,
      aux_sym_int_token1,
    STATE(125), 1,
      sym_line_comment,
  [3449] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(310), 1,
      anon_sym_rel,
    STATE(126), 1,
      sym_line_comment,
  [3465] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(312), 1,
      anon_sym_ptr,
    STATE(127), 1,
      sym_line_comment,
  [3481] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(314), 1,
      anon_sym_RPAREN,
    STATE(128), 1,
      sym_line_comment,
  [3497] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(316), 1,
      anon_sym_RPAREN,
    STATE(129), 1,
      sym_line_comment,
  [3513] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(318), 1,
      ts_builtin_sym_end,
    STATE(130), 1,
      sym_line_comment,
  [3529] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(23), 1,
      anon_sym_COLON,
    STATE(131), 1,
      sym_line_comment,
  [3545] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(320), 1,
      sym_word,
    STATE(132), 1,
      sym_line_comment,
  [3561] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(322), 1,
      sym_word,
    STATE(133), 1,
      sym_line_comment,
  [3577] = 5,
    ACTIONS(3), 1,
      anon_sym_POUND,
    ACTIONS(5), 1,
      aux_sym_line_comment_token2,
    ACTIONS(7), 1,
      sym_block_comment,
    ACTIONS(324), 1,
      aux_sym_int_token1,
    STATE(134), 1,
      sym_line_comment,
  [3593] = 1,
    ACTIONS(326), 1,
      ts_builtin_sym_end,
  [3597] = 1,
    ACTIONS(328), 1,
      ts_builtin_sym_end,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(3)] = 0,
  [SMALL_STATE(4)] = 69,
  [SMALL_STATE(5)] = 138,
  [SMALL_STATE(6)] = 203,
  [SMALL_STATE(7)] = 238,
  [SMALL_STATE(8)] = 279,
  [SMALL_STATE(9)] = 314,
  [SMALL_STATE(10)] = 348,
  [SMALL_STATE(11)] = 386,
  [SMALL_STATE(12)] = 420,
  [SMALL_STATE(13)] = 454,
  [SMALL_STATE(14)] = 493,
  [SMALL_STATE(15)] = 536,
  [SMALL_STATE(16)] = 569,
  [SMALL_STATE(17)] = 606,
  [SMALL_STATE(18)] = 639,
  [SMALL_STATE(19)] = 686,
  [SMALL_STATE(20)] = 721,
  [SMALL_STATE(21)] = 769,
  [SMALL_STATE(22)] = 815,
  [SMALL_STATE(23)] = 866,
  [SMALL_STATE(24)] = 914,
  [SMALL_STATE(25)] = 955,
  [SMALL_STATE(26)] = 996,
  [SMALL_STATE(27)] = 1037,
  [SMALL_STATE(28)] = 1078,
  [SMALL_STATE(29)] = 1119,
  [SMALL_STATE(30)] = 1160,
  [SMALL_STATE(31)] = 1201,
  [SMALL_STATE(32)] = 1242,
  [SMALL_STATE(33)] = 1283,
  [SMALL_STATE(34)] = 1324,
  [SMALL_STATE(35)] = 1365,
  [SMALL_STATE(36)] = 1396,
  [SMALL_STATE(37)] = 1426,
  [SMALL_STATE(38)] = 1472,
  [SMALL_STATE(39)] = 1518,
  [SMALL_STATE(40)] = 1548,
  [SMALL_STATE(41)] = 1578,
  [SMALL_STATE(42)] = 1621,
  [SMALL_STATE(43)] = 1649,
  [SMALL_STATE(44)] = 1676,
  [SMALL_STATE(45)] = 1705,
  [SMALL_STATE(46)] = 1736,
  [SMALL_STATE(47)] = 1773,
  [SMALL_STATE(48)] = 1806,
  [SMALL_STATE(49)] = 1841,
  [SMALL_STATE(50)] = 1868,
  [SMALL_STATE(51)] = 1900,
  [SMALL_STATE(52)] = 1932,
  [SMALL_STATE(53)] = 1959,
  [SMALL_STATE(54)] = 1983,
  [SMALL_STATE(55)] = 2013,
  [SMALL_STATE(56)] = 2043,
  [SMALL_STATE(57)] = 2069,
  [SMALL_STATE(58)] = 2095,
  [SMALL_STATE(59)] = 2121,
  [SMALL_STATE(60)] = 2147,
  [SMALL_STATE(61)] = 2173,
  [SMALL_STATE(62)] = 2196,
  [SMALL_STATE(63)] = 2221,
  [SMALL_STATE(64)] = 2242,
  [SMALL_STATE(65)] = 2265,
  [SMALL_STATE(66)] = 2288,
  [SMALL_STATE(67)] = 2311,
  [SMALL_STATE(68)] = 2334,
  [SMALL_STATE(69)] = 2357,
  [SMALL_STATE(70)] = 2380,
  [SMALL_STATE(71)] = 2403,
  [SMALL_STATE(72)] = 2426,
  [SMALL_STATE(73)] = 2449,
  [SMALL_STATE(74)] = 2472,
  [SMALL_STATE(75)] = 2495,
  [SMALL_STATE(76)] = 2518,
  [SMALL_STATE(77)] = 2541,
  [SMALL_STATE(78)] = 2562,
  [SMALL_STATE(79)] = 2583,
  [SMALL_STATE(80)] = 2606,
  [SMALL_STATE(81)] = 2627,
  [SMALL_STATE(82)] = 2652,
  [SMALL_STATE(83)] = 2677,
  [SMALL_STATE(84)] = 2698,
  [SMALL_STATE(85)] = 2719,
  [SMALL_STATE(86)] = 2744,
  [SMALL_STATE(87)] = 2765,
  [SMALL_STATE(88)] = 2786,
  [SMALL_STATE(89)] = 2806,
  [SMALL_STATE(90)] = 2824,
  [SMALL_STATE(91)] = 2842,
  [SMALL_STATE(92)] = 2860,
  [SMALL_STATE(93)] = 2878,
  [SMALL_STATE(94)] = 2896,
  [SMALL_STATE(95)] = 2914,
  [SMALL_STATE(96)] = 2932,
  [SMALL_STATE(97)] = 2950,
  [SMALL_STATE(98)] = 2968,
  [SMALL_STATE(99)] = 2986,
  [SMALL_STATE(100)] = 3004,
  [SMALL_STATE(101)] = 3022,
  [SMALL_STATE(102)] = 3040,
  [SMALL_STATE(103)] = 3060,
  [SMALL_STATE(104)] = 3078,
  [SMALL_STATE(105)] = 3096,
  [SMALL_STATE(106)] = 3115,
  [SMALL_STATE(107)] = 3132,
  [SMALL_STATE(108)] = 3149,
  [SMALL_STATE(109)] = 3166,
  [SMALL_STATE(110)] = 3185,
  [SMALL_STATE(111)] = 3202,
  [SMALL_STATE(112)] = 3219,
  [SMALL_STATE(113)] = 3236,
  [SMALL_STATE(114)] = 3253,
  [SMALL_STATE(115)] = 3270,
  [SMALL_STATE(116)] = 3289,
  [SMALL_STATE(117)] = 3305,
  [SMALL_STATE(118)] = 3321,
  [SMALL_STATE(119)] = 3337,
  [SMALL_STATE(120)] = 3353,
  [SMALL_STATE(121)] = 3369,
  [SMALL_STATE(122)] = 3385,
  [SMALL_STATE(123)] = 3401,
  [SMALL_STATE(124)] = 3417,
  [SMALL_STATE(125)] = 3433,
  [SMALL_STATE(126)] = 3449,
  [SMALL_STATE(127)] = 3465,
  [SMALL_STATE(128)] = 3481,
  [SMALL_STATE(129)] = 3497,
  [SMALL_STATE(130)] = 3513,
  [SMALL_STATE(131)] = 3529,
  [SMALL_STATE(132)] = 3545,
  [SMALL_STATE(133)] = 3561,
  [SMALL_STATE(134)] = 3577,
  [SMALL_STATE(135)] = 3593,
  [SMALL_STATE(136)] = 3597,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, SHIFT(116),
  [5] = {.entry = {.count = 1, .reusable = true}}, SHIFT(136),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT_EXTRA(),
  [9] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_program, 0, 0, 0),
  [11] = {.entry = {.count = 1, .reusable = false}}, SHIFT(133),
  [13] = {.entry = {.count = 1, .reusable = false}}, SHIFT(132),
  [15] = {.entry = {.count = 1, .reusable = false}}, SHIFT(2),
  [17] = {.entry = {.count = 1, .reusable = false}}, SHIFT(22),
  [19] = {.entry = {.count = 1, .reusable = false}}, SHIFT(131),
  [21] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_instruction, 1, 0, 1),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(58),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [27] = {.entry = {.count = 1, .reusable = true}}, SHIFT(56),
  [29] = {.entry = {.count = 1, .reusable = false}}, SHIFT(127),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(79),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(126),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(125),
  [37] = {.entry = {.count = 1, .reusable = false}}, SHIFT(8),
  [39] = {.entry = {.count = 1, .reusable = false}}, SHIFT(100),
  [41] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [43] = {.entry = {.count = 1, .reusable = false}}, SHIFT(9),
  [45] = {.entry = {.count = 1, .reusable = false}}, SHIFT(11),
  [47] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_instruction, 3, 0, 1),
  [49] = {.entry = {.count = 1, .reusable = true}}, SHIFT(134),
  [51] = {.entry = {.count = 1, .reusable = false}}, SHIFT(36),
  [53] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [55] = {.entry = {.count = 1, .reusable = false}}, SHIFT(35),
  [57] = {.entry = {.count = 1, .reusable = false}}, SHIFT(40),
  [59] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_instruction, 4, 0, 1),
  [61] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_int, 2, 0, 0),
  [63] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_int, 2, 0, 0),
  [65] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__expr, 1, 0, 0), REDUCE(sym__tc_expr, 1, 0, 0),
  [68] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__expr, 1, 0, 0),
  [70] = {.entry = {.count = 1, .reusable = true}}, SHIFT(72),
  [72] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__tc_expr, 1, 0, 0),
  [74] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__tc_expr, 1, 0, 0),
  [76] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_int, 1, 0, 0),
  [78] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_int, 1, 0, 0),
  [80] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_reg, 1, 0, 0),
  [82] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_reg, 1, 0, 0),
  [84] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ident, 1, 0, 0),
  [86] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_ident, 1, 0, 0),
  [88] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 1, 0, 0),
  [90] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 1, 0, 0),
  [92] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_tc_infix, 3, 0, 5),
  [94] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_tc_infix, 3, 0, 5),
  [96] = {.entry = {.count = 1, .reusable = true}}, SHIFT(28),
  [98] = {.entry = {.count = 1, .reusable = true}}, SHIFT(29),
  [100] = {.entry = {.count = 1, .reusable = true}}, SHIFT(31),
  [102] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [104] = {.entry = {.count = 1, .reusable = false}}, SHIFT(27),
  [106] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_instruction_repeat2, 1, 0, 0),
  [108] = {.entry = {.count = 1, .reusable = false}}, SHIFT(25),
  [110] = {.entry = {.count = 1, .reusable = true}}, SHIFT(25),
  [112] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_instruction_repeat2, 1, 0, 0),
  [114] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_instruction, 2, 0, 1),
  [116] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_instruction_repeat2, 2, 0, 0),
  [118] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(125),
  [121] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(8),
  [124] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(12),
  [127] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(9),
  [130] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(11),
  [133] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_meta, 1, 0, 1),
  [135] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [137] = {.entry = {.count = 1, .reusable = false}}, SHIFT(70),
  [139] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_program, 2, 0, 0),
  [141] = {.entry = {.count = 1, .reusable = true}}, SHIFT(53),
  [143] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_program, 3, 0, 0),
  [145] = {.entry = {.count = 1, .reusable = true}}, SHIFT(24),
  [147] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [149] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_const, 3, 0, 4),
  [151] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [153] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [155] = {.entry = {.count = 1, .reusable = false}}, SHIFT(33),
  [157] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [159] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0),
  [161] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0), SHIFT_REPEAT(53),
  [164] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_program_repeat1, 2, 0, 0),
  [166] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_program_repeat1, 1, 0, 0),
  [168] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_program_repeat1, 1, 0, 0),
  [170] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [172] = {.entry = {.count = 1, .reusable = true}}, SHIFT(98),
  [174] = {.entry = {.count = 1, .reusable = true}}, SHIFT(94),
  [176] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 2, 0, 3),
  [178] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [180] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [182] = {.entry = {.count = 1, .reusable = true}}, SHIFT(103),
  [184] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 2, 0, 0),
  [186] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [188] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_program_repeat2, 2, 0, 0),
  [190] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_program_repeat2, 2, 0, 0), SHIFT_REPEAT(53),
  [193] = {.entry = {.count = 1, .reusable = true}}, SHIFT(57),
  [195] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [197] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_list_repeat1, 2, 0, 0), SHIFT_REPEAT(73),
  [200] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_list_repeat1, 2, 0, 0),
  [202] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_meta, 3, 0, 1),
  [204] = {.entry = {.count = 1, .reusable = true}}, SHIFT(124),
  [206] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_meta, 2, 0, 1),
  [208] = {.entry = {.count = 1, .reusable = true}}, SHIFT(88),
  [210] = {.entry = {.count = 1, .reusable = true}}, SHIFT(105),
  [212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [214] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(109),
  [218] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [220] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [222] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ptr, 3, 0, 0),
  [224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(96),
  [226] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_instruction_repeat1, 2, 0, 0),
  [228] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(5),
  [231] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_program, 1, 0, 0),
  [233] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [235] = {.entry = {.count = 1, .reusable = true}}, SHIFT(89),
  [237] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ptr, 5, 0, 0),
  [239] = {.entry = {.count = 1, .reusable = true}}, SHIFT(90),
  [241] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_meta_repeat3, 2, 0, 0),
  [243] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_meta_repeat3, 2, 0, 0), SHIFT_REPEAT(88),
  [246] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_meta_repeat2, 2, 0, 0),
  [248] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_meta_repeat2, 2, 0, 0), SHIFT_REPEAT(124),
  [251] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_meta_repeat1, 2, 0, 0),
  [253] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_meta_repeat1, 2, 0, 0), SHIFT_REPEAT(105),
  [256] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 3, 0, 0),
  [258] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ptr, 6, 0, 0),
  [260] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 5, 0, 0),
  [262] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ptr, 4, 0, 0),
  [264] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 2, 0, 0),
  [266] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_ptr, 7, 0, 0),
  [268] = {.entry = {.count = 1, .reusable = true}}, SHIFT(51),
  [270] = {.entry = {.count = 1, .reusable = true}}, SHIFT(93),
  [272] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 4, 0, 0),
  [274] = {.entry = {.count = 1, .reusable = true}}, SHIFT(36),
  [276] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 5, 0, 3),
  [278] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 3, 0, 3),
  [280] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 3, 0, 0),
  [282] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 5, 0, 0),
  [284] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label, 2, 0, 2),
  [286] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__item, 1, 0, 0),
  [288] = {.entry = {.count = 1, .reusable = false}}, SHIFT(116),
  [290] = {.entry = {.count = 1, .reusable = false}}, SHIFT(135),
  [292] = {.entry = {.count = 1, .reusable = false}}, SHIFT(136),
  [294] = {.entry = {.count = 1, .reusable = false}}, SHIFT_EXTRA(),
  [296] = {.entry = {.count = 1, .reusable = true}}, SHIFT(101),
  [298] = {.entry = {.count = 1, .reusable = true}}, SHIFT(115),
  [300] = {.entry = {.count = 1, .reusable = true}}, SHIFT(75),
  [302] = {.entry = {.count = 1, .reusable = true}}, SHIFT(92),
  [304] = {.entry = {.count = 1, .reusable = true}}, SHIFT(83),
  [306] = {.entry = {.count = 1, .reusable = true}}, SHIFT(95),
  [308] = {.entry = {.count = 1, .reusable = true}}, SHIFT(6),
  [310] = {.entry = {.count = 1, .reusable = true}}, SHIFT(118),
  [312] = {.entry = {.count = 1, .reusable = true}}, SHIFT(119),
  [314] = {.entry = {.count = 1, .reusable = true}}, SHIFT(107),
  [316] = {.entry = {.count = 1, .reusable = true}}, SHIFT(112),
  [318] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [320] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [322] = {.entry = {.count = 1, .reusable = true}}, SHIFT(113),
  [324] = {.entry = {.count = 1, .reusable = true}}, SHIFT(39),
  [326] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_line_comment, 2, 0, 0),
  [328] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_line_comment, 1, 0, 0),
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

TS_PUBLIC const TSLanguage *tree_sitter_asm(void) {
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
    .field_names = ts_field_names,
    .field_map_slices = ts_field_map_slices,
    .field_map_entries = ts_field_map_entries,
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
