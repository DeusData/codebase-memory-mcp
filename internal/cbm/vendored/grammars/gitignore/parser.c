#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#define LANGUAGE_VERSION 13
#define STATE_COUNT 62
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 51
#define ALIAS_COUNT 0
#define TOKEN_COUNT 32
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 3
#define MAX_ALIAS_SEQUENCE_LENGTH 5
#define PRODUCTION_ID_COUNT 19

enum {
  sym_comment = 1,
  anon_sym_BANG = 2,
  sym_directory_separator = 3,
  sym_directory_separator_escaped = 4,
  sym_pattern_char = 5,
  anon_sym_BSLASH = 6,
  aux_sym_pattern_char_escaped_token1 = 7,
  anon_sym_QMARK = 8,
  anon_sym_STAR = 9,
  anon_sym_STAR_STAR = 10,
  anon_sym_LBRACK = 11,
  anon_sym_CARET = 12,
  anon_sym_RBRACK = 13,
  anon_sym_DASH = 14,
  sym_bracket_char = 15,
  anon_sym_LBRACK_COLON = 16,
  anon_sym_alnum = 17,
  anon_sym_COLON_RBRACK = 18,
  anon_sym_alpha = 19,
  anon_sym_blank = 20,
  anon_sym_cntrl = 21,
  anon_sym_digit = 22,
  anon_sym_graph = 23,
  anon_sym_lower = 24,
  anon_sym_print = 25,
  anon_sym_punct = 26,
  anon_sym_space = 27,
  anon_sym_upper = 28,
  anon_sym_xdigit = 29,
  sym__trailing_spaces = 30,
  sym__newline = 31,
  sym_document = 32,
  sym__line = 33,
  sym_pattern = 34,
  sym__directory_separator = 35,
  aux_sym__pattern = 36,
  sym_pattern_char_escaped = 37,
  sym__wildcard = 38,
  sym_bracket_expr = 39,
  sym__bracket_pattern = 40,
  sym__bracket_pattern_closing_bracket = 41,
  sym__bracket_char_closing_bracket = 42,
  sym__bracket_range_closing_bracket = 43,
  sym__bracket_char = 44,
  sym_bracket_char_escaped = 45,
  sym_bracket_range = 46,
  sym_bracket_char_class = 47,
  aux_sym_document_repeat1 = 48,
  aux_sym_pattern_repeat1 = 49,
  aux_sym_bracket_expr_repeat1 = 50,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [sym_comment] = "comment",
  [anon_sym_BANG] = "negation",
  [sym_directory_separator] = "directory_separator",
  [sym_directory_separator_escaped] = "directory_separator_escaped",
  [sym_pattern_char] = "pattern_char",
  [anon_sym_BSLASH] = "\\",
  [aux_sym_pattern_char_escaped_token1] = "pattern_char_escaped_token1",
  [anon_sym_QMARK] = "wildcard_char_single",
  [anon_sym_STAR] = "wildcard_chars",
  [anon_sym_STAR_STAR] = "wildcard_chars_allow_slash",
  [anon_sym_LBRACK] = "[",
  [anon_sym_CARET] = "bracket_negation",
  [anon_sym_RBRACK] = "]",
  [anon_sym_DASH] = "-",
  [sym_bracket_char] = "bracket_char",
  [anon_sym_LBRACK_COLON] = "[:",
  [anon_sym_alnum] = "alnum",
  [anon_sym_COLON_RBRACK] = ":]",
  [anon_sym_alpha] = "alpha",
  [anon_sym_blank] = "blank",
  [anon_sym_cntrl] = "cntrl",
  [anon_sym_digit] = "digit",
  [anon_sym_graph] = "graph",
  [anon_sym_lower] = "lower",
  [anon_sym_print] = "print",
  [anon_sym_punct] = "punct",
  [anon_sym_space] = "space",
  [anon_sym_upper] = "upper",
  [anon_sym_xdigit] = "xdigit",
  [sym__trailing_spaces] = "_trailing_spaces",
  [sym__newline] = "_newline",
  [sym_document] = "document",
  [sym__line] = "_line",
  [sym_pattern] = "pattern",
  [sym__directory_separator] = "_directory_separator",
  [aux_sym__pattern] = "_pattern",
  [sym_pattern_char_escaped] = "pattern_char_escaped",
  [sym__wildcard] = "_wildcard",
  [sym_bracket_expr] = "bracket_expr",
  [sym__bracket_pattern] = "_bracket_pattern",
  [sym__bracket_pattern_closing_bracket] = "_bracket_pattern_closing_bracket",
  [sym__bracket_char_closing_bracket] = "_bracket_char_closing_bracket",
  [sym__bracket_range_closing_bracket] = "bracket_range",
  [sym__bracket_char] = "_bracket_char",
  [sym_bracket_char_escaped] = "bracket_char_escaped",
  [sym_bracket_range] = "bracket_range",
  [sym_bracket_char_class] = "bracket_char_class",
  [aux_sym_document_repeat1] = "document_repeat1",
  [aux_sym_pattern_repeat1] = "pattern_repeat1",
  [aux_sym_bracket_expr_repeat1] = "bracket_expr_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [sym_comment] = sym_comment,
  [anon_sym_BANG] = anon_sym_BANG,
  [sym_directory_separator] = sym_directory_separator,
  [sym_directory_separator_escaped] = sym_directory_separator_escaped,
  [sym_pattern_char] = sym_pattern_char,
  [anon_sym_BSLASH] = anon_sym_BSLASH,
  [aux_sym_pattern_char_escaped_token1] = aux_sym_pattern_char_escaped_token1,
  [anon_sym_QMARK] = anon_sym_QMARK,
  [anon_sym_STAR] = anon_sym_STAR,
  [anon_sym_STAR_STAR] = anon_sym_STAR_STAR,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_CARET] = anon_sym_CARET,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_DASH] = anon_sym_DASH,
  [sym_bracket_char] = sym_bracket_char,
  [anon_sym_LBRACK_COLON] = anon_sym_LBRACK_COLON,
  [anon_sym_alnum] = anon_sym_alnum,
  [anon_sym_COLON_RBRACK] = anon_sym_COLON_RBRACK,
  [anon_sym_alpha] = anon_sym_alpha,
  [anon_sym_blank] = anon_sym_blank,
  [anon_sym_cntrl] = anon_sym_cntrl,
  [anon_sym_digit] = anon_sym_digit,
  [anon_sym_graph] = anon_sym_graph,
  [anon_sym_lower] = anon_sym_lower,
  [anon_sym_print] = anon_sym_print,
  [anon_sym_punct] = anon_sym_punct,
  [anon_sym_space] = anon_sym_space,
  [anon_sym_upper] = anon_sym_upper,
  [anon_sym_xdigit] = anon_sym_xdigit,
  [sym__trailing_spaces] = sym__trailing_spaces,
  [sym__newline] = sym__newline,
  [sym_document] = sym_document,
  [sym__line] = sym__line,
  [sym_pattern] = sym_pattern,
  [sym__directory_separator] = sym__directory_separator,
  [aux_sym__pattern] = aux_sym__pattern,
  [sym_pattern_char_escaped] = sym_pattern_char_escaped,
  [sym__wildcard] = sym__wildcard,
  [sym_bracket_expr] = sym_bracket_expr,
  [sym__bracket_pattern] = sym__bracket_pattern,
  [sym__bracket_pattern_closing_bracket] = sym__bracket_pattern_closing_bracket,
  [sym__bracket_char_closing_bracket] = sym__bracket_char_closing_bracket,
  [sym__bracket_range_closing_bracket] = sym_bracket_range,
  [sym__bracket_char] = sym__bracket_char,
  [sym_bracket_char_escaped] = sym_bracket_char_escaped,
  [sym_bracket_range] = sym_bracket_range,
  [sym_bracket_char_class] = sym_bracket_char_class,
  [aux_sym_document_repeat1] = aux_sym_document_repeat1,
  [aux_sym_pattern_repeat1] = aux_sym_pattern_repeat1,
  [aux_sym_bracket_expr_repeat1] = aux_sym_bracket_expr_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_BANG] = {
    .visible = true,
    .named = true,
  },
  [sym_directory_separator] = {
    .visible = true,
    .named = true,
  },
  [sym_directory_separator_escaped] = {
    .visible = true,
    .named = true,
  },
  [sym_pattern_char] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_BSLASH] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_pattern_char_escaped_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_QMARK] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_STAR] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_STAR_STAR] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_CARET] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH] = {
    .visible = true,
    .named = false,
  },
  [sym_bracket_char] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LBRACK_COLON] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_alnum] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COLON_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_alpha] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_blank] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_cntrl] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_digit] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_graph] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_lower] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_print] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_punct] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_space] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_upper] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_xdigit] = {
    .visible = true,
    .named = false,
  },
  [sym__trailing_spaces] = {
    .visible = false,
    .named = true,
  },
  [sym__newline] = {
    .visible = false,
    .named = true,
  },
  [sym_document] = {
    .visible = true,
    .named = true,
  },
  [sym__line] = {
    .visible = false,
    .named = true,
  },
  [sym_pattern] = {
    .visible = true,
    .named = true,
  },
  [sym__directory_separator] = {
    .visible = false,
    .named = true,
  },
  [aux_sym__pattern] = {
    .visible = false,
    .named = false,
  },
  [sym_pattern_char_escaped] = {
    .visible = true,
    .named = true,
  },
  [sym__wildcard] = {
    .visible = false,
    .named = true,
  },
  [sym_bracket_expr] = {
    .visible = true,
    .named = true,
  },
  [sym__bracket_pattern] = {
    .visible = false,
    .named = true,
  },
  [sym__bracket_pattern_closing_bracket] = {
    .visible = false,
    .named = true,
  },
  [sym__bracket_char_closing_bracket] = {
    .visible = false,
    .named = true,
  },
  [sym__bracket_range_closing_bracket] = {
    .visible = true,
    .named = true,
  },
  [sym__bracket_char] = {
    .visible = false,
    .named = true,
  },
  [sym_bracket_char_escaped] = {
    .visible = true,
    .named = true,
  },
  [sym_bracket_range] = {
    .visible = true,
    .named = true,
  },
  [sym_bracket_char_class] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_document_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_pattern_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_bracket_expr_repeat1] = {
    .visible = false,
    .named = false,
  },
};

enum {
  field_directory_flag = 1,
  field_name = 2,
  field_relative_flag = 3,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_directory_flag] = "directory_flag",
  [field_name] = "name",
  [field_relative_flag] = "relative_flag",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [2] = {.index = 0, .length = 1},
  [3] = {.index = 1, .length = 1},
  [4] = {.index = 2, .length = 1},
  [5] = {.index = 3, .length = 1},
  [6] = {.index = 4, .length = 1},
  [7] = {.index = 5, .length = 1},
  [8] = {.index = 6, .length = 2},
  [9] = {.index = 8, .length = 2},
  [10] = {.index = 10, .length = 2},
  [11] = {.index = 12, .length = 2},
  [12] = {.index = 14, .length = 2},
  [13] = {.index = 16, .length = 2},
  [14] = {.index = 18, .length = 2},
  [16] = {.index = 20, .length = 1},
  [17] = {.index = 21, .length = 3},
  [18] = {.index = 24, .length = 3},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_relative_flag, 0},
  [1] =
    {field_directory_flag, 1},
  [2] =
    {field_relative_flag, 1, .inherited = true},
  [3] =
    {field_relative_flag, 1},
  [4] =
    {field_directory_flag, 2},
  [5] =
    {field_relative_flag, 2, .inherited = true},
  [6] =
    {field_directory_flag, 2},
    {field_relative_flag, 0},
  [8] =
    {field_relative_flag, 0},
    {field_relative_flag, 2, .inherited = true},
  [10] =
    {field_directory_flag, 2},
    {field_relative_flag, 1, .inherited = true},
  [12] =
    {field_relative_flag, 0, .inherited = true},
    {field_relative_flag, 1, .inherited = true},
  [14] =
    {field_directory_flag, 3},
    {field_relative_flag, 1},
  [16] =
    {field_relative_flag, 1},
    {field_relative_flag, 3, .inherited = true},
  [18] =
    {field_directory_flag, 3},
    {field_relative_flag, 2, .inherited = true},
  [20] =
    {field_name, 1},
  [21] =
    {field_directory_flag, 3},
    {field_relative_flag, 0},
    {field_relative_flag, 2, .inherited = true},
  [24] =
    {field_directory_flag, 4},
    {field_relative_flag, 1},
    {field_relative_flag, 3, .inherited = true},
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
  [1] = {
    [0] = sym_bracket_char,
  },
  [15] = {
    [1] = anon_sym_CARET,
  },
};

static const uint16_t ts_non_terminal_alias_map[] = {
  0,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(61);
      if (lookahead == '!') ADVANCE(63);
      if (lookahead == '#') ADVANCE(62);
      if (lookahead == '*') ADVANCE(73);
      if (lookahead == '-') ADVANCE(79);
      if (lookahead == '/') ADVANCE(64);
      if (lookahead == '?') ADVANCE(72);
      if (lookahead == '[') ADVANCE(76);
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead == ']') ADVANCE(78);
      if (lookahead == '^') ADVANCE(77);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(66);
      END_STATE();
    case 1:
      if (lookahead == '\n') ADVANCE(97);
      END_STATE();
    case 2:
      if (lookahead == '\n') ADVANCE(97);
      if (lookahead == '\r') ADVANCE(67);
      if (lookahead == ' ') ADVANCE(68);
      if (lookahead == '*') ADVANCE(74);
      if (lookahead == '/') ADVANCE(64);
      if (lookahead == '?') ADVANCE(72);
      if (lookahead == '[') ADVANCE(76);
      if (lookahead == '\\') ADVANCE(70);
      if (lookahead != 0) ADVANCE(66);
      END_STATE();
    case 3:
      if (lookahead == '\n') ADVANCE(97);
      if (lookahead == '\r') ADVANCE(67);
      if (lookahead == ' ') ADVANCE(68);
      if (lookahead == '*') ADVANCE(74);
      if (lookahead == '?') ADVANCE(72);
      if (lookahead == '[') ADVANCE(76);
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead != 0 &&
          lookahead != '/') ADVANCE(66);
      END_STATE();
    case 4:
      if (lookahead == '\n') ADVANCE(97);
      if (lookahead == '\r') ADVANCE(1);
      if (lookahead == ' ') ADVANCE(96);
      if (lookahead == '/') ADVANCE(64);
      if (lookahead == ':') ADVANCE(12);
      if (lookahead == '\\') ADVANCE(9);
      if (lookahead == 'a') ADVANCE(33);
      if (lookahead == 'b') ADVANCE(34);
      if (lookahead == 'c') ADVANCE(37);
      if (lookahead == 'd') ADVANCE(27);
      if (lookahead == 'g') ADVANCE(50);
      if (lookahead == 'l') ADVANCE(42);
      if (lookahead == 'p') ADVANCE(49);
      if (lookahead == 's') ADVANCE(45);
      if (lookahead == 'u') ADVANCE(44);
      if (lookahead == 'x') ADVANCE(19);
      END_STATE();
    case 5:
      if (lookahead == '!') ADVANCE(63);
      if (lookahead == '[') ADVANCE(81);
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead == ']') ADVANCE(78);
      if (lookahead == '^') ADVANCE(77);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '/') ADVANCE(80);
      END_STATE();
    case 6:
      if (lookahead == '*') ADVANCE(74);
      if (lookahead == '/') ADVANCE(64);
      if (lookahead == '?') ADVANCE(72);
      if (lookahead == '[') ADVANCE(76);
      if (lookahead == '\\') ADVANCE(70);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(66);
      END_STATE();
    case 7:
      if (lookahead == '*') ADVANCE(74);
      if (lookahead == '?') ADVANCE(72);
      if (lookahead == '[') ADVANCE(76);
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '/') ADVANCE(66);
      END_STATE();
    case 8:
      if (lookahead == '-') ADVANCE(79);
      if (lookahead == '[') ADVANCE(81);
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead == ']') ADVANCE(78);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '/') ADVANCE(80);
      END_STATE();
    case 9:
      if (lookahead == '/') ADVANCE(65);
      END_STATE();
    case 10:
      if (lookahead == '[') ADVANCE(81);
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead == ']') ADVANCE(78);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '/') ADVANCE(80);
      END_STATE();
    case 11:
      if (lookahead == '\\') ADVANCE(69);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '/' &&
          lookahead != ']') ADVANCE(80);
      END_STATE();
    case 12:
      if (lookahead == ']') ADVANCE(84);
      END_STATE();
    case 13:
      if (lookahead == 'a') ADVANCE(85);
      END_STATE();
    case 14:
      if (lookahead == 'a') ADVANCE(17);
      END_STATE();
    case 15:
      if (lookahead == 'a') ADVANCE(40);
      END_STATE();
    case 16:
      if (lookahead == 'a') ADVANCE(43);
      END_STATE();
    case 17:
      if (lookahead == 'c') ADVANCE(20);
      END_STATE();
    case 18:
      if (lookahead == 'c') ADVANCE(54);
      END_STATE();
    case 19:
      if (lookahead == 'd') ADVANCE(31);
      END_STATE();
    case 20:
      if (lookahead == 'e') ADVANCE(93);
      END_STATE();
    case 21:
      if (lookahead == 'e') ADVANCE(47);
      END_STATE();
    case 22:
      if (lookahead == 'e') ADVANCE(48);
      END_STATE();
    case 23:
      if (lookahead == 'g') ADVANCE(28);
      END_STATE();
    case 24:
      if (lookahead == 'g') ADVANCE(29);
      END_STATE();
    case 25:
      if (lookahead == 'h') ADVANCE(89);
      END_STATE();
    case 26:
      if (lookahead == 'h') ADVANCE(13);
      END_STATE();
    case 27:
      if (lookahead == 'i') ADVANCE(23);
      END_STATE();
    case 28:
      if (lookahead == 'i') ADVANCE(52);
      END_STATE();
    case 29:
      if (lookahead == 'i') ADVANCE(55);
      END_STATE();
    case 30:
      if (lookahead == 'i') ADVANCE(41);
      END_STATE();
    case 31:
      if (lookahead == 'i') ADVANCE(24);
      END_STATE();
    case 32:
      if (lookahead == 'k') ADVANCE(86);
      END_STATE();
    case 33:
      if (lookahead == 'l') ADVANCE(38);
      END_STATE();
    case 34:
      if (lookahead == 'l') ADVANCE(15);
      END_STATE();
    case 35:
      if (lookahead == 'l') ADVANCE(87);
      END_STATE();
    case 36:
      if (lookahead == 'm') ADVANCE(83);
      END_STATE();
    case 37:
      if (lookahead == 'n') ADVANCE(56);
      END_STATE();
    case 38:
      if (lookahead == 'n') ADVANCE(57);
      if (lookahead == 'p') ADVANCE(26);
      END_STATE();
    case 39:
      if (lookahead == 'n') ADVANCE(18);
      END_STATE();
    case 40:
      if (lookahead == 'n') ADVANCE(32);
      END_STATE();
    case 41:
      if (lookahead == 'n') ADVANCE(53);
      END_STATE();
    case 42:
      if (lookahead == 'o') ADVANCE(58);
      END_STATE();
    case 43:
      if (lookahead == 'p') ADVANCE(25);
      END_STATE();
    case 44:
      if (lookahead == 'p') ADVANCE(46);
      END_STATE();
    case 45:
      if (lookahead == 'p') ADVANCE(14);
      END_STATE();
    case 46:
      if (lookahead == 'p') ADVANCE(22);
      END_STATE();
    case 47:
      if (lookahead == 'r') ADVANCE(90);
      END_STATE();
    case 48:
      if (lookahead == 'r') ADVANCE(94);
      END_STATE();
    case 49:
      if (lookahead == 'r') ADVANCE(30);
      if (lookahead == 'u') ADVANCE(39);
      END_STATE();
    case 50:
      if (lookahead == 'r') ADVANCE(16);
      END_STATE();
    case 51:
      if (lookahead == 'r') ADVANCE(35);
      END_STATE();
    case 52:
      if (lookahead == 't') ADVANCE(88);
      END_STATE();
    case 53:
      if (lookahead == 't') ADVANCE(91);
      END_STATE();
    case 54:
      if (lookahead == 't') ADVANCE(92);
      END_STATE();
    case 55:
      if (lookahead == 't') ADVANCE(95);
      END_STATE();
    case 56:
      if (lookahead == 't') ADVANCE(51);
      END_STATE();
    case 57:
      if (lookahead == 'u') ADVANCE(36);
      END_STATE();
    case 58:
      if (lookahead == 'w') ADVANCE(21);
      END_STATE();
    case 59:
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '/') ADVANCE(71);
      END_STATE();
    case 60:
      if (eof) ADVANCE(61);
      if (lookahead == '\n') ADVANCE(97);
      if (lookahead == '\r') ADVANCE(67);
      if (lookahead == ' ') ADVANCE(68);
      if (lookahead == '!') ADVANCE(63);
      if (lookahead == '#') ADVANCE(62);
      if (lookahead == '*') ADVANCE(74);
      if (lookahead == '/') ADVANCE(64);
      if (lookahead == '?') ADVANCE(72);
      if (lookahead == '[') ADVANCE(76);
      if (lookahead == '\\') ADVANCE(70);
      if (lookahead != 0) ADVANCE(66);
      END_STATE();
    case 61:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 62:
      ACCEPT_TOKEN(sym_comment);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(62);
      END_STATE();
    case 63:
      ACCEPT_TOKEN(anon_sym_BANG);
      END_STATE();
    case 64:
      ACCEPT_TOKEN(sym_directory_separator);
      END_STATE();
    case 65:
      ACCEPT_TOKEN(sym_directory_separator_escaped);
      END_STATE();
    case 66:
      ACCEPT_TOKEN(sym_pattern_char);
      END_STATE();
    case 67:
      ACCEPT_TOKEN(sym_pattern_char);
      if (lookahead == '\n') ADVANCE(97);
      END_STATE();
    case 68:
      ACCEPT_TOKEN(sym_pattern_char);
      if (lookahead == ' ') ADVANCE(96);
      END_STATE();
    case 69:
      ACCEPT_TOKEN(anon_sym_BSLASH);
      END_STATE();
    case 70:
      ACCEPT_TOKEN(anon_sym_BSLASH);
      if (lookahead == '/') ADVANCE(65);
      END_STATE();
    case 71:
      ACCEPT_TOKEN(aux_sym_pattern_char_escaped_token1);
      END_STATE();
    case 72:
      ACCEPT_TOKEN(anon_sym_QMARK);
      END_STATE();
    case 73:
      ACCEPT_TOKEN(anon_sym_STAR);
      END_STATE();
    case 74:
      ACCEPT_TOKEN(anon_sym_STAR);
      if (lookahead == '*') ADVANCE(75);
      END_STATE();
    case 75:
      ACCEPT_TOKEN(anon_sym_STAR_STAR);
      END_STATE();
    case 76:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 77:
      ACCEPT_TOKEN(anon_sym_CARET);
      END_STATE();
    case 78:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 79:
      ACCEPT_TOKEN(anon_sym_DASH);
      END_STATE();
    case 80:
      ACCEPT_TOKEN(sym_bracket_char);
      END_STATE();
    case 81:
      ACCEPT_TOKEN(sym_bracket_char);
      if (lookahead == ':') ADVANCE(82);
      END_STATE();
    case 82:
      ACCEPT_TOKEN(anon_sym_LBRACK_COLON);
      END_STATE();
    case 83:
      ACCEPT_TOKEN(anon_sym_alnum);
      END_STATE();
    case 84:
      ACCEPT_TOKEN(anon_sym_COLON_RBRACK);
      END_STATE();
    case 85:
      ACCEPT_TOKEN(anon_sym_alpha);
      END_STATE();
    case 86:
      ACCEPT_TOKEN(anon_sym_blank);
      END_STATE();
    case 87:
      ACCEPT_TOKEN(anon_sym_cntrl);
      END_STATE();
    case 88:
      ACCEPT_TOKEN(anon_sym_digit);
      END_STATE();
    case 89:
      ACCEPT_TOKEN(anon_sym_graph);
      END_STATE();
    case 90:
      ACCEPT_TOKEN(anon_sym_lower);
      END_STATE();
    case 91:
      ACCEPT_TOKEN(anon_sym_print);
      END_STATE();
    case 92:
      ACCEPT_TOKEN(anon_sym_punct);
      END_STATE();
    case 93:
      ACCEPT_TOKEN(anon_sym_space);
      END_STATE();
    case 94:
      ACCEPT_TOKEN(anon_sym_upper);
      END_STATE();
    case 95:
      ACCEPT_TOKEN(anon_sym_xdigit);
      END_STATE();
    case 96:
      ACCEPT_TOKEN(sym__trailing_spaces);
      if (lookahead == ' ') ADVANCE(96);
      END_STATE();
    case 97:
      ACCEPT_TOKEN(sym__newline);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 60},
  [2] = {.lex_state = 60},
  [3] = {.lex_state = 60},
  [4] = {.lex_state = 2},
  [5] = {.lex_state = 2},
  [6] = {.lex_state = 2},
  [7] = {.lex_state = 2},
  [8] = {.lex_state = 5},
  [9] = {.lex_state = 2},
  [10] = {.lex_state = 2},
  [11] = {.lex_state = 60},
  [12] = {.lex_state = 60},
  [13] = {.lex_state = 10},
  [14] = {.lex_state = 6},
  [15] = {.lex_state = 3},
  [16] = {.lex_state = 3},
  [17] = {.lex_state = 3},
  [18] = {.lex_state = 3},
  [19] = {.lex_state = 3},
  [20] = {.lex_state = 4},
  [21] = {.lex_state = 3},
  [22] = {.lex_state = 3},
  [23] = {.lex_state = 3},
  [24] = {.lex_state = 10},
  [25] = {.lex_state = 10},
  [26] = {.lex_state = 2},
  [27] = {.lex_state = 7},
  [28] = {.lex_state = 2},
  [29] = {.lex_state = 7},
  [30] = {.lex_state = 2},
  [31] = {.lex_state = 10},
  [32] = {.lex_state = 2},
  [33] = {.lex_state = 10},
  [34] = {.lex_state = 10},
  [35] = {.lex_state = 10},
  [36] = {.lex_state = 7},
  [37] = {.lex_state = 2},
  [38] = {.lex_state = 10},
  [39] = {.lex_state = 4},
  [40] = {.lex_state = 4},
  [41] = {.lex_state = 4},
  [42] = {.lex_state = 4},
  [43] = {.lex_state = 4},
  [44] = {.lex_state = 8},
  [45] = {.lex_state = 8},
  [46] = {.lex_state = 8},
  [47] = {.lex_state = 8},
  [48] = {.lex_state = 11},
  [49] = {.lex_state = 10},
  [50] = {.lex_state = 10},
  [51] = {.lex_state = 10},
  [52] = {.lex_state = 11},
  [53] = {.lex_state = 10},
  [54] = {.lex_state = 4},
  [55] = {.lex_state = 60},
  [56] = {.lex_state = 4},
  [57] = {.lex_state = 59},
  [58] = {.lex_state = 59},
  [59] = {.lex_state = 60},
  [60] = {.lex_state = 0},
  [61] = {.lex_state = 59},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [sym_comment] = ACTIONS(1),
    [anon_sym_BANG] = ACTIONS(1),
    [sym_directory_separator] = ACTIONS(1),
    [sym_pattern_char] = ACTIONS(1),
    [anon_sym_BSLASH] = ACTIONS(1),
    [aux_sym_pattern_char_escaped_token1] = ACTIONS(1),
    [anon_sym_QMARK] = ACTIONS(1),
    [anon_sym_STAR] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_CARET] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_DASH] = ACTIONS(1),
    [sym_bracket_char] = ACTIONS(1),
  },
  [1] = {
    [sym_document] = STATE(60),
    [sym__line] = STATE(3),
    [sym_pattern] = STATE(54),
    [sym__directory_separator] = STATE(27),
    [aux_sym__pattern] = STATE(7),
    [sym_pattern_char_escaped] = STATE(7),
    [sym__wildcard] = STATE(7),
    [sym_bracket_expr] = STATE(7),
    [aux_sym_document_repeat1] = STATE(3),
    [ts_builtin_sym_end] = ACTIONS(3),
    [sym_comment] = ACTIONS(5),
    [anon_sym_BANG] = ACTIONS(7),
    [sym_directory_separator] = ACTIONS(9),
    [sym_directory_separator_escaped] = ACTIONS(9),
    [sym_pattern_char] = ACTIONS(11),
    [anon_sym_BSLASH] = ACTIONS(13),
    [anon_sym_QMARK] = ACTIONS(15),
    [anon_sym_STAR] = ACTIONS(11),
    [anon_sym_STAR_STAR] = ACTIONS(15),
    [anon_sym_LBRACK] = ACTIONS(17),
    [sym__trailing_spaces] = ACTIONS(19),
    [sym__newline] = ACTIONS(21),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 14,
    ACTIONS(23), 1,
      ts_builtin_sym_end,
    ACTIONS(25), 1,
      sym_comment,
    ACTIONS(28), 1,
      anon_sym_BANG,
    ACTIONS(37), 1,
      anon_sym_BSLASH,
    ACTIONS(43), 1,
      anon_sym_LBRACK,
    ACTIONS(46), 1,
      sym__trailing_spaces,
    ACTIONS(49), 1,
      sym__newline,
    STATE(27), 1,
      sym__directory_separator,
    STATE(54), 1,
      sym_pattern,
    ACTIONS(31), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(34), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(40), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(2), 2,
      sym__line,
      aux_sym_document_repeat1,
    STATE(7), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [50] = 14,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(7), 1,
      anon_sym_BANG,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(19), 1,
      sym__trailing_spaces,
    ACTIONS(52), 1,
      ts_builtin_sym_end,
    ACTIONS(54), 1,
      sym__newline,
    STATE(27), 1,
      sym__directory_separator,
    STATE(54), 1,
      sym_pattern,
    ACTIONS(9), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(11), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(15), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(2), 2,
      sym__line,
      aux_sym_document_repeat1,
    STATE(7), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [100] = 10,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(62), 1,
      sym__trailing_spaces,
    ACTIONS(64), 1,
      sym__newline,
    STATE(15), 1,
      sym__directory_separator,
    STATE(42), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(56), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(58), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(60), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(10), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [137] = 10,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(68), 1,
      sym__trailing_spaces,
    ACTIONS(70), 1,
      sym__newline,
    STATE(19), 1,
      sym__directory_separator,
    STATE(43), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(58), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(60), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    ACTIONS(66), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    STATE(10), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [174] = 10,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(74), 1,
      sym__trailing_spaces,
    ACTIONS(76), 1,
      sym__newline,
    STATE(22), 1,
      sym__directory_separator,
    STATE(39), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(58), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(60), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    ACTIONS(72), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    STATE(10), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [211] = 10,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(80), 1,
      sym__trailing_spaces,
    ACTIONS(82), 1,
      sym__newline,
    STATE(23), 1,
      sym__directory_separator,
    STATE(41), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(58), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(60), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    ACTIONS(78), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    STATE(10), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [248] = 9,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(88), 1,
      anon_sym_RBRACK,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    STATE(46), 1,
      sym__bracket_char_closing_bracket,
    ACTIONS(84), 2,
      anon_sym_BANG,
      anon_sym_CARET,
    STATE(35), 2,
      sym__bracket_pattern_closing_bracket,
      sym__bracket_range_closing_bracket,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(24), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [282] = 7,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(96), 1,
      sym__trailing_spaces,
    ACTIONS(58), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(60), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    ACTIONS(94), 3,
      sym_directory_separator,
      sym_directory_separator_escaped,
      sym__newline,
    STATE(10), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [311] = 7,
    ACTIONS(103), 1,
      anon_sym_BSLASH,
    ACTIONS(109), 1,
      anon_sym_LBRACK,
    ACTIONS(112), 1,
      sym__trailing_spaces,
    ACTIONS(100), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(106), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    ACTIONS(98), 3,
      sym_directory_separator,
      sym_directory_separator_escaped,
      sym__newline,
    STATE(10), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [340] = 2,
    ACTIONS(116), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(114), 9,
      ts_builtin_sym_end,
      sym_comment,
      anon_sym_BANG,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [358] = 2,
    ACTIONS(120), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(118), 9,
      ts_builtin_sym_end,
      sym_comment,
      anon_sym_BANG,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [376] = 8,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(88), 1,
      anon_sym_RBRACK,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    STATE(46), 1,
      sym__bracket_char_closing_bracket,
    STATE(33), 2,
      sym__bracket_pattern_closing_bracket,
      sym__bracket_range_closing_bracket,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(34), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [406] = 7,
    ACTIONS(13), 1,
      anon_sym_BSLASH,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    STATE(36), 1,
      sym__directory_separator,
    ACTIONS(122), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(124), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(126), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(4), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [434] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(134), 1,
      sym__trailing_spaces,
    ACTIONS(136), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [461] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(138), 1,
      sym__trailing_spaces,
    ACTIONS(140), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [488] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(142), 1,
      sym__trailing_spaces,
    ACTIONS(144), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [515] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(146), 1,
      sym__trailing_spaces,
    ACTIONS(148), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [542] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(150), 1,
      sym__trailing_spaces,
    ACTIONS(152), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [569] = 1,
    ACTIONS(154), 12,
      anon_sym_alnum,
      anon_sym_alpha,
      anon_sym_blank,
      anon_sym_cntrl,
      anon_sym_digit,
      anon_sym_graph,
      anon_sym_lower,
      anon_sym_print,
      anon_sym_punct,
      anon_sym_space,
      anon_sym_upper,
      anon_sym_xdigit,
  [584] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(156), 1,
      sym__trailing_spaces,
    ACTIONS(158), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [611] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(160), 1,
      sym__trailing_spaces,
    ACTIONS(162), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [638] = 7,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(164), 1,
      sym__trailing_spaces,
    ACTIONS(166), 1,
      sym__newline,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [665] = 6,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    ACTIONS(168), 1,
      anon_sym_RBRACK,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(25), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [688] = 6,
    ACTIONS(170), 1,
      anon_sym_BSLASH,
    ACTIONS(173), 1,
      anon_sym_RBRACK,
    ACTIONS(175), 1,
      sym_bracket_char,
    ACTIONS(178), 1,
      anon_sym_LBRACK_COLON,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(25), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [711] = 2,
    ACTIONS(183), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(181), 6,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [726] = 5,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(185), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(187), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(6), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [747] = 2,
    ACTIONS(191), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(189), 6,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [762] = 5,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(128), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(132), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(9), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [783] = 2,
    ACTIONS(195), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(193), 6,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [798] = 6,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    ACTIONS(197), 1,
      anon_sym_RBRACK,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(25), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [821] = 2,
    ACTIONS(201), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(199), 6,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [836] = 6,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    ACTIONS(203), 1,
      anon_sym_RBRACK,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(31), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [859] = 6,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    ACTIONS(203), 1,
      anon_sym_RBRACK,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(25), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [882] = 6,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    ACTIONS(168), 1,
      anon_sym_RBRACK,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(38), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [905] = 5,
    ACTIONS(17), 1,
      anon_sym_LBRACK,
    ACTIONS(130), 1,
      anon_sym_BSLASH,
    ACTIONS(205), 2,
      sym_pattern_char,
      anon_sym_STAR,
    ACTIONS(207), 2,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
    STATE(5), 4,
      aux_sym__pattern,
      sym_pattern_char_escaped,
      sym__wildcard,
      sym_bracket_expr,
  [926] = 2,
    ACTIONS(211), 4,
      sym_pattern_char,
      anon_sym_BSLASH,
      anon_sym_STAR,
      sym__trailing_spaces,
    ACTIONS(209), 6,
      sym_directory_separator,
      sym_directory_separator_escaped,
      anon_sym_QMARK,
      anon_sym_STAR_STAR,
      anon_sym_LBRACK,
      sym__newline,
  [941] = 6,
    ACTIONS(86), 1,
      anon_sym_BSLASH,
    ACTIONS(90), 1,
      sym_bracket_char,
    ACTIONS(92), 1,
      anon_sym_LBRACK_COLON,
    ACTIONS(213), 1,
      anon_sym_RBRACK,
    STATE(45), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
    STATE(25), 4,
      sym__bracket_pattern,
      sym_bracket_range,
      sym_bracket_char_class,
      aux_sym_bracket_expr_repeat1,
  [964] = 4,
    STATE(17), 1,
      sym__directory_separator,
    STATE(40), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(215), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(217), 2,
      sym__trailing_spaces,
      sym__newline,
  [979] = 4,
    STATE(29), 1,
      sym__directory_separator,
    STATE(40), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(219), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(222), 2,
      sym__trailing_spaces,
      sym__newline,
  [994] = 4,
    STATE(21), 1,
      sym__directory_separator,
    STATE(40), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(224), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(226), 2,
      sym__trailing_spaces,
      sym__newline,
  [1009] = 4,
    STATE(18), 1,
      sym__directory_separator,
    STATE(40), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(228), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(230), 2,
      sym__trailing_spaces,
      sym__newline,
  [1024] = 4,
    STATE(16), 1,
      sym__directory_separator,
    STATE(40), 1,
      aux_sym_pattern_repeat1,
    ACTIONS(232), 2,
      sym_directory_separator,
      sym_directory_separator_escaped,
    ACTIONS(234), 2,
      sym__trailing_spaces,
      sym__newline,
  [1039] = 2,
    ACTIONS(238), 1,
      sym_bracket_char,
    ACTIONS(236), 4,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_DASH,
      anon_sym_LBRACK_COLON,
  [1049] = 3,
    ACTIONS(242), 1,
      anon_sym_DASH,
    ACTIONS(244), 1,
      sym_bracket_char,
    ACTIONS(240), 3,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_LBRACK_COLON,
  [1061] = 3,
    ACTIONS(248), 1,
      anon_sym_DASH,
    ACTIONS(250), 1,
      sym_bracket_char,
    ACTIONS(246), 3,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_LBRACK_COLON,
  [1073] = 2,
    ACTIONS(254), 1,
      sym_bracket_char,
    ACTIONS(252), 4,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_DASH,
      anon_sym_LBRACK_COLON,
  [1083] = 3,
    ACTIONS(256), 1,
      anon_sym_BSLASH,
    ACTIONS(258), 1,
      sym_bracket_char,
    STATE(50), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
  [1094] = 2,
    ACTIONS(262), 1,
      sym_bracket_char,
    ACTIONS(260), 3,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_LBRACK_COLON,
  [1103] = 2,
    ACTIONS(266), 1,
      sym_bracket_char,
    ACTIONS(264), 3,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_LBRACK_COLON,
  [1112] = 2,
    ACTIONS(270), 1,
      sym_bracket_char,
    ACTIONS(268), 3,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_LBRACK_COLON,
  [1121] = 3,
    ACTIONS(256), 1,
      anon_sym_BSLASH,
    ACTIONS(272), 1,
      sym_bracket_char,
    STATE(51), 2,
      sym__bracket_char,
      sym_bracket_char_escaped,
  [1132] = 2,
    ACTIONS(238), 1,
      sym_bracket_char,
    ACTIONS(236), 3,
      anon_sym_BSLASH,
      anon_sym_RBRACK,
      anon_sym_LBRACK_COLON,
  [1141] = 2,
    ACTIONS(274), 1,
      sym__trailing_spaces,
    ACTIONS(276), 1,
      sym__newline,
  [1148] = 1,
    ACTIONS(278), 1,
      sym__newline,
  [1152] = 1,
    ACTIONS(280), 1,
      anon_sym_COLON_RBRACK,
  [1156] = 1,
    ACTIONS(282), 1,
      aux_sym_pattern_char_escaped_token1,
  [1160] = 1,
    ACTIONS(284), 1,
      aux_sym_pattern_char_escaped_token1,
  [1164] = 1,
    ACTIONS(276), 1,
      sym__newline,
  [1168] = 1,
    ACTIONS(286), 1,
      ts_builtin_sym_end,
  [1172] = 1,
    ACTIONS(288), 1,
      aux_sym_pattern_char_escaped_token1,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 50,
  [SMALL_STATE(4)] = 100,
  [SMALL_STATE(5)] = 137,
  [SMALL_STATE(6)] = 174,
  [SMALL_STATE(7)] = 211,
  [SMALL_STATE(8)] = 248,
  [SMALL_STATE(9)] = 282,
  [SMALL_STATE(10)] = 311,
  [SMALL_STATE(11)] = 340,
  [SMALL_STATE(12)] = 358,
  [SMALL_STATE(13)] = 376,
  [SMALL_STATE(14)] = 406,
  [SMALL_STATE(15)] = 434,
  [SMALL_STATE(16)] = 461,
  [SMALL_STATE(17)] = 488,
  [SMALL_STATE(18)] = 515,
  [SMALL_STATE(19)] = 542,
  [SMALL_STATE(20)] = 569,
  [SMALL_STATE(21)] = 584,
  [SMALL_STATE(22)] = 611,
  [SMALL_STATE(23)] = 638,
  [SMALL_STATE(24)] = 665,
  [SMALL_STATE(25)] = 688,
  [SMALL_STATE(26)] = 711,
  [SMALL_STATE(27)] = 726,
  [SMALL_STATE(28)] = 747,
  [SMALL_STATE(29)] = 762,
  [SMALL_STATE(30)] = 783,
  [SMALL_STATE(31)] = 798,
  [SMALL_STATE(32)] = 821,
  [SMALL_STATE(33)] = 836,
  [SMALL_STATE(34)] = 859,
  [SMALL_STATE(35)] = 882,
  [SMALL_STATE(36)] = 905,
  [SMALL_STATE(37)] = 926,
  [SMALL_STATE(38)] = 941,
  [SMALL_STATE(39)] = 964,
  [SMALL_STATE(40)] = 979,
  [SMALL_STATE(41)] = 994,
  [SMALL_STATE(42)] = 1009,
  [SMALL_STATE(43)] = 1024,
  [SMALL_STATE(44)] = 1039,
  [SMALL_STATE(45)] = 1049,
  [SMALL_STATE(46)] = 1061,
  [SMALL_STATE(47)] = 1073,
  [SMALL_STATE(48)] = 1083,
  [SMALL_STATE(49)] = 1094,
  [SMALL_STATE(50)] = 1103,
  [SMALL_STATE(51)] = 1112,
  [SMALL_STATE(52)] = 1121,
  [SMALL_STATE(53)] = 1132,
  [SMALL_STATE(54)] = 1141,
  [SMALL_STATE(55)] = 1148,
  [SMALL_STATE(56)] = 1152,
  [SMALL_STATE(57)] = 1156,
  [SMALL_STATE(58)] = 1160,
  [SMALL_STATE(59)] = 1164,
  [SMALL_STATE(60)] = 1168,
  [SMALL_STATE(61)] = 1172,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_document, 0),
  [5] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [11] = {.entry = {.count = 1, .reusable = false}}, SHIFT(7),
  [13] = {.entry = {.count = 1, .reusable = false}}, SHIFT(57),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [19] = {.entry = {.count = 1, .reusable = false}}, SHIFT(59),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [23] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2),
  [25] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(54),
  [28] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(14),
  [31] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(27),
  [34] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(7),
  [37] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(57),
  [40] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(7),
  [43] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(8),
  [46] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(59),
  [49] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_document_repeat1, 2), SHIFT_REPEAT(2),
  [52] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_document, 1),
  [54] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [56] = {.entry = {.count = 1, .reusable = true}}, SHIFT(15),
  [58] = {.entry = {.count = 1, .reusable = false}}, SHIFT(10),
  [60] = {.entry = {.count = 1, .reusable = true}}, SHIFT(10),
  [62] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 2),
  [64] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 2),
  [66] = {.entry = {.count = 1, .reusable = true}}, SHIFT(19),
  [68] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 3, .production_id = 5),
  [70] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 3, .production_id = 5),
  [72] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [74] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 2, .production_id = 2),
  [76] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 2, .production_id = 2),
  [78] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [80] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 1),
  [82] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 1),
  [84] = {.entry = {.count = 1, .reusable = true}}, SHIFT(13),
  [86] = {.entry = {.count = 1, .reusable = true}}, SHIFT(58),
  [88] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [90] = {.entry = {.count = 1, .reusable = false}}, SHIFT(45),
  [92] = {.entry = {.count = 1, .reusable = true}}, SHIFT(20),
  [94] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_pattern_repeat1, 2, .production_id = 2),
  [96] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_pattern_repeat1, 2, .production_id = 2),
  [98] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__pattern, 2),
  [100] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__pattern, 2), SHIFT_REPEAT(10),
  [103] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__pattern, 2), SHIFT_REPEAT(57),
  [106] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__pattern, 2), SHIFT_REPEAT(10),
  [109] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__pattern, 2), SHIFT_REPEAT(8),
  [112] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym__pattern, 2),
  [114] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__line, 3),
  [116] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__line, 3),
  [118] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__line, 2),
  [120] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__line, 2),
  [122] = {.entry = {.count = 1, .reusable = true}}, SHIFT(36),
  [124] = {.entry = {.count = 1, .reusable = false}}, SHIFT(4),
  [126] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [128] = {.entry = {.count = 1, .reusable = false}}, SHIFT(9),
  [130] = {.entry = {.count = 1, .reusable = true}}, SHIFT(57),
  [132] = {.entry = {.count = 1, .reusable = true}}, SHIFT(9),
  [134] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 3, .production_id = 6),
  [136] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 3, .production_id = 6),
  [138] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 5, .production_id = 18),
  [140] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 5, .production_id = 18),
  [142] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 4, .production_id = 17),
  [144] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 4, .production_id = 17),
  [146] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 4, .production_id = 14),
  [148] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 4, .production_id = 14),
  [150] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 4, .production_id = 12),
  [152] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 4, .production_id = 12),
  [154] = {.entry = {.count = 1, .reusable = true}}, SHIFT(56),
  [156] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 3, .production_id = 10),
  [158] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 3, .production_id = 10),
  [160] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 3, .production_id = 8),
  [162] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 3, .production_id = 8),
  [164] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern, 2, .production_id = 3),
  [166] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 2, .production_id = 3),
  [168] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [170] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_bracket_expr_repeat1, 2), SHIFT_REPEAT(58),
  [173] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_bracket_expr_repeat1, 2),
  [175] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_bracket_expr_repeat1, 2), SHIFT_REPEAT(45),
  [178] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_bracket_expr_repeat1, 2), SHIFT_REPEAT(20),
  [181] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_expr, 5, .production_id = 15),
  [183] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_expr, 5, .production_id = 15),
  [185] = {.entry = {.count = 1, .reusable = false}}, SHIFT(6),
  [187] = {.entry = {.count = 1, .reusable = true}}, SHIFT(6),
  [189] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern_char_escaped, 2),
  [191] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pattern_char_escaped, 2),
  [193] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_expr, 4),
  [195] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_expr, 4),
  [197] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [199] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_expr, 4, .production_id = 15),
  [201] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_expr, 4, .production_id = 15),
  [203] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [205] = {.entry = {.count = 1, .reusable = false}}, SHIFT(5),
  [207] = {.entry = {.count = 1, .reusable = true}}, SHIFT(5),
  [209] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_expr, 3),
  [211] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_expr, 3),
  [213] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [215] = {.entry = {.count = 1, .reusable = true}}, SHIFT(17),
  [217] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 3, .production_id = 9),
  [219] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_pattern_repeat1, 2, .production_id = 11), SHIFT_REPEAT(29),
  [222] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_pattern_repeat1, 2, .production_id = 11),
  [224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(21),
  [226] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 2, .production_id = 4),
  [228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(18),
  [230] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 3, .production_id = 7),
  [232] = {.entry = {.count = 1, .reusable = true}}, SHIFT(16),
  [234] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pattern, 4, .production_id = 13),
  [236] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_char_escaped, 2),
  [238] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_char_escaped, 2),
  [240] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__bracket_pattern, 1),
  [242] = {.entry = {.count = 1, .reusable = true}}, SHIFT(52),
  [244] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__bracket_pattern, 1),
  [246] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__bracket_pattern_closing_bracket, 1),
  [248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [250] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__bracket_pattern_closing_bracket, 1),
  [252] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__bracket_char_closing_bracket, 1, .production_id = 1),
  [254] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__bracket_char_closing_bracket, 1, .production_id = 1),
  [256] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [258] = {.entry = {.count = 1, .reusable = false}}, SHIFT(50),
  [260] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_char_class, 3, .production_id = 16),
  [262] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_char_class, 3, .production_id = 16),
  [264] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__bracket_range_closing_bracket, 3),
  [266] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__bracket_range_closing_bracket, 3),
  [268] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bracket_range, 3),
  [270] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_bracket_range, 3),
  [272] = {.entry = {.count = 1, .reusable = false}}, SHIFT(51),
  [274] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [276] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [278] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [280] = {.entry = {.count = 1, .reusable = true}}, SHIFT(49),
  [282] = {.entry = {.count = 1, .reusable = true}}, SHIFT(28),
  [284] = {.entry = {.count = 1, .reusable = true}}, SHIFT(44),
  [286] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [288] = {.entry = {.count = 1, .reusable = true}}, SHIFT(53),
};

#ifdef __cplusplus
extern "C" {
#endif
#ifdef _WIN32
#define extern __declspec(dllexport)
#endif

extern const TSLanguage *tree_sitter_gitignore(void) {
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
  };
  return &language;
}
#ifdef __cplusplus
}
#endif
