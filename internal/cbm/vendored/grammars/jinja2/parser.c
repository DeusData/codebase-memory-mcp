#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 104
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 44
#define ALIAS_COUNT 0
#define TOKEN_COUNT 26
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 4
#define MAX_ALIAS_SEQUENCE_LENGTH 5
#define PRODUCTION_ID_COUNT 3

enum {
  anon_sym_LBRACE_LBRACE = 1,
  anon_sym_RBRACE_RBRACE = 2,
  anon_sym_LBRACE_PERCENT = 3,
  aux_sym_jinja_expression_token1 = 4,
  anon_sym_LBRACE_POUND = 5,
  aux_sym__jinja_comment_token1 = 6,
  anon_sym_LPAREN = 7,
  anon_sym_COMMA = 8,
  anon_sym_RPAREN = 9,
  anon_sym_SQUOTE = 10,
  aux_sym_lit_string_token1 = 11,
  anon_sym_DQUOTE = 12,
  aux_sym_lit_string_token2 = 13,
  anon_sym_True = 14,
  anon_sym_False = 15,
  anon_sym_LBRACK = 16,
  anon_sym_RBRACK = 17,
  anon_sym_LBRACE = 18,
  anon_sym_RBRACE = 19,
  anon_sym_COLON = 20,
  sym__identifier = 21,
  anon_sym_EQ = 22,
  sym__text = 23,
  sym_integer = 24,
  sym_float = 25,
  sym_source_file = 26,
  sym__jinja_value = 27,
  sym_jinja_expression = 28,
  sym__jinja_comment = 29,
  sym__expr = 30,
  sym_fn_call = 31,
  sym_argument_list = 32,
  sym_lit_string = 33,
  sym_bool = 34,
  sym_list = 35,
  sym_dict = 36,
  sym_pair = 37,
  sym_identifier = 38,
  sym_kwarg = 39,
  aux_sym_source_file_repeat1 = 40,
  aux_sym_argument_list_repeat1 = 41,
  aux_sym_list_repeat1 = 42,
  aux_sym_dict_repeat1 = 43,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_LBRACE_LBRACE] = "{{",
  [anon_sym_RBRACE_RBRACE] = "}}",
  [anon_sym_LBRACE_PERCENT] = "{%",
  [aux_sym_jinja_expression_token1] = "jinja_expression_token1",
  [anon_sym_LBRACE_POUND] = "{#",
  [aux_sym__jinja_comment_token1] = "_jinja_comment_token1",
  [anon_sym_LPAREN] = "(",
  [anon_sym_COMMA] = ",",
  [anon_sym_RPAREN] = ")",
  [anon_sym_SQUOTE] = "'",
  [aux_sym_lit_string_token1] = "lit_string_token1",
  [anon_sym_DQUOTE] = "\"",
  [aux_sym_lit_string_token2] = "lit_string_token2",
  [anon_sym_True] = "True",
  [anon_sym_False] = "False",
  [anon_sym_LBRACK] = "[",
  [anon_sym_RBRACK] = "]",
  [anon_sym_LBRACE] = "{",
  [anon_sym_RBRACE] = "}",
  [anon_sym_COLON] = ":",
  [sym__identifier] = "_identifier",
  [anon_sym_EQ] = "=",
  [sym__text] = "_text",
  [sym_integer] = "integer",
  [sym_float] = "float",
  [sym_source_file] = "source_file",
  [sym__jinja_value] = "_jinja_value",
  [sym_jinja_expression] = "jinja_expression",
  [sym__jinja_comment] = "_jinja_comment",
  [sym__expr] = "_expr",
  [sym_fn_call] = "fn_call",
  [sym_argument_list] = "argument_list",
  [sym_lit_string] = "lit_string",
  [sym_bool] = "bool",
  [sym_list] = "list",
  [sym_dict] = "dict",
  [sym_pair] = "pair",
  [sym_identifier] = "identifier",
  [sym_kwarg] = "kwarg",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym_argument_list_repeat1] = "argument_list_repeat1",
  [aux_sym_list_repeat1] = "list_repeat1",
  [aux_sym_dict_repeat1] = "dict_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_LBRACE_LBRACE] = anon_sym_LBRACE_LBRACE,
  [anon_sym_RBRACE_RBRACE] = anon_sym_RBRACE_RBRACE,
  [anon_sym_LBRACE_PERCENT] = anon_sym_LBRACE_PERCENT,
  [aux_sym_jinja_expression_token1] = aux_sym_jinja_expression_token1,
  [anon_sym_LBRACE_POUND] = anon_sym_LBRACE_POUND,
  [aux_sym__jinja_comment_token1] = aux_sym__jinja_comment_token1,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [anon_sym_SQUOTE] = anon_sym_SQUOTE,
  [aux_sym_lit_string_token1] = aux_sym_lit_string_token1,
  [anon_sym_DQUOTE] = anon_sym_DQUOTE,
  [aux_sym_lit_string_token2] = aux_sym_lit_string_token2,
  [anon_sym_True] = anon_sym_True,
  [anon_sym_False] = anon_sym_False,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_LBRACE] = anon_sym_LBRACE,
  [anon_sym_RBRACE] = anon_sym_RBRACE,
  [anon_sym_COLON] = anon_sym_COLON,
  [sym__identifier] = sym__identifier,
  [anon_sym_EQ] = anon_sym_EQ,
  [sym__text] = sym__text,
  [sym_integer] = sym_integer,
  [sym_float] = sym_float,
  [sym_source_file] = sym_source_file,
  [sym__jinja_value] = sym__jinja_value,
  [sym_jinja_expression] = sym_jinja_expression,
  [sym__jinja_comment] = sym__jinja_comment,
  [sym__expr] = sym__expr,
  [sym_fn_call] = sym_fn_call,
  [sym_argument_list] = sym_argument_list,
  [sym_lit_string] = sym_lit_string,
  [sym_bool] = sym_bool,
  [sym_list] = sym_list,
  [sym_dict] = sym_dict,
  [sym_pair] = sym_pair,
  [sym_identifier] = sym_identifier,
  [sym_kwarg] = sym_kwarg,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym_argument_list_repeat1] = aux_sym_argument_list_repeat1,
  [aux_sym_list_repeat1] = aux_sym_list_repeat1,
  [aux_sym_dict_repeat1] = aux_sym_dict_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_LBRACE_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE_PERCENT] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_jinja_expression_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_LBRACE_POUND] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__jinja_comment_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_lit_string_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_lit_string_token2] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_True] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_False] = {
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
  [anon_sym_COLON] = {
    .visible = true,
    .named = false,
  },
  [sym__identifier] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_EQ] = {
    .visible = true,
    .named = false,
  },
  [sym__text] = {
    .visible = false,
    .named = true,
  },
  [sym_integer] = {
    .visible = true,
    .named = true,
  },
  [sym_float] = {
    .visible = true,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym__jinja_value] = {
    .visible = false,
    .named = true,
  },
  [sym_jinja_expression] = {
    .visible = true,
    .named = true,
  },
  [sym__jinja_comment] = {
    .visible = false,
    .named = true,
  },
  [sym__expr] = {
    .visible = false,
    .named = true,
  },
  [sym_fn_call] = {
    .visible = true,
    .named = true,
  },
  [sym_argument_list] = {
    .visible = true,
    .named = true,
  },
  [sym_lit_string] = {
    .visible = true,
    .named = true,
  },
  [sym_bool] = {
    .visible = true,
    .named = true,
  },
  [sym_list] = {
    .visible = true,
    .named = true,
  },
  [sym_dict] = {
    .visible = true,
    .named = true,
  },
  [sym_pair] = {
    .visible = true,
    .named = true,
  },
  [sym_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_kwarg] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_argument_list_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_list_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_dict_repeat1] = {
    .visible = false,
    .named = false,
  },
};

enum {
  field_argument_list = 1,
  field_fn_name = 2,
  field_key = 3,
  field_value = 4,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_argument_list] = "argument_list",
  [field_fn_name] = "fn_name",
  [field_key] = "key",
  [field_value] = "value",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [1] = {.index = 0, .length = 2},
  [2] = {.index = 2, .length = 2},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_argument_list, 1},
    {field_fn_name, 0},
  [2] =
    {field_key, 0},
    {field_value, 2},
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
  [3] = 2,
  [4] = 4,
  [5] = 5,
  [6] = 6,
  [7] = 5,
  [8] = 4,
  [9] = 6,
  [10] = 10,
  [11] = 11,
  [12] = 11,
  [13] = 10,
  [14] = 14,
  [15] = 15,
  [16] = 16,
  [17] = 17,
  [18] = 18,
  [19] = 19,
  [20] = 20,
  [21] = 21,
  [22] = 21,
  [23] = 23,
  [24] = 24,
  [25] = 25,
  [26] = 25,
  [27] = 27,
  [28] = 24,
  [29] = 29,
  [30] = 30,
  [31] = 31,
  [32] = 32,
  [33] = 33,
  [34] = 34,
  [35] = 35,
  [36] = 36,
  [37] = 37,
  [38] = 38,
  [39] = 39,
  [40] = 40,
  [41] = 41,
  [42] = 42,
  [43] = 43,
  [44] = 44,
  [45] = 45,
  [46] = 46,
  [47] = 47,
  [48] = 48,
  [49] = 49,
  [50] = 50,
  [51] = 51,
  [52] = 52,
  [53] = 49,
  [54] = 54,
  [55] = 50,
  [56] = 56,
  [57] = 52,
  [58] = 56,
  [59] = 46,
  [60] = 60,
  [61] = 60,
  [62] = 43,
  [63] = 63,
  [64] = 64,
  [65] = 65,
  [66] = 41,
  [67] = 37,
  [68] = 44,
  [69] = 36,
  [70] = 30,
  [71] = 32,
  [72] = 34,
  [73] = 39,
  [74] = 42,
  [75] = 75,
  [76] = 45,
  [77] = 38,
  [78] = 33,
  [79] = 35,
  [80] = 31,
  [81] = 81,
  [82] = 63,
  [83] = 83,
  [84] = 84,
  [85] = 85,
  [86] = 86,
  [87] = 87,
  [88] = 88,
  [89] = 88,
  [90] = 87,
  [91] = 91,
  [92] = 92,
  [93] = 85,
  [94] = 94,
  [95] = 94,
  [96] = 96,
  [97] = 97,
  [98] = 98,
  [99] = 99,
  [100] = 86,
  [101] = 101,
  [102] = 98,
  [103] = 97,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(20);
      if (lookahead == '"') ADVANCE(35);
      if (lookahead == '\'') ADVANCE(31);
      if (lookahead == '(') ADVANCE(28);
      if (lookahead == ')') ADVANCE(30);
      if (lookahead == ',') ADVANCE(29);
      if (lookahead == '.') ADVANCE(15);
      if (lookahead == ':') ADVANCE(46);
      if (lookahead == '=') ADVANCE(57);
      if (lookahead == 'F') ADVANCE(48);
      if (lookahead == 'T') ADVANCE(52);
      if (lookahead == '[') ADVANCE(41);
      if (lookahead == ']') ADVANCE(42);
      if (lookahead == '_') ADVANCE(55);
      if (lookahead == '{') ADVANCE(44);
      if (lookahead == '}') ADVANCE(45);
      if (('+' <= lookahead && lookahead <= '-')) ADVANCE(9);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(0)
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(60);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 1:
      if (lookahead == '"') ADVANCE(35);
      if (lookahead == '\'') ADVANCE(31);
      if (lookahead == ')') ADVANCE(30);
      if (lookahead == ',') ADVANCE(29);
      if (lookahead == '.') ADVANCE(15);
      if (lookahead == ':') ADVANCE(46);
      if (lookahead == 'F') ADVANCE(48);
      if (lookahead == 'T') ADVANCE(52);
      if (lookahead == '[') ADVANCE(41);
      if (lookahead == ']') ADVANCE(42);
      if (lookahead == '_') ADVANCE(55);
      if (lookahead == '{') ADVANCE(43);
      if (lookahead == '}') ADVANCE(12);
      if (('+' <= lookahead && lookahead <= '-')) ADVANCE(9);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') SKIP(1)
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(60);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 2:
      if (lookahead == '#') ADVANCE(25);
      if (lookahead == '%') ADVANCE(23);
      if (lookahead == '{') ADVANCE(21);
      if (lookahead != 0) ADVANCE(59);
      END_STATE();
    case 3:
      if (lookahead == '#') ADVANCE(6);
      if (lookahead == '}') ADVANCE(27);
      if (lookahead != 0) ADVANCE(5);
      END_STATE();
    case 4:
      if (lookahead == '#') ADVANCE(6);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(4);
      if (lookahead != 0) ADVANCE(5);
      END_STATE();
    case 5:
      if (lookahead == '#') ADVANCE(6);
      if (lookahead != 0) ADVANCE(5);
      END_STATE();
    case 6:
      if (lookahead == '#') ADVANCE(3);
      if (lookahead == '}') ADVANCE(26);
      if (lookahead != 0) ADVANCE(5);
      END_STATE();
    case 7:
      if (lookahead == '%') ADVANCE(13);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(7);
      if (lookahead != 0) ADVANCE(8);
      END_STATE();
    case 8:
      if (lookahead == '%') ADVANCE(13);
      if (lookahead != 0) ADVANCE(8);
      END_STATE();
    case 9:
      if (lookahead == '.') ADVANCE(15);
      if (lookahead == '_') ADVANCE(16);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(61);
      END_STATE();
    case 10:
      if (lookahead == '.') ADVANCE(65);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(14);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(60);
      END_STATE();
    case 11:
      if (lookahead == '.') ADVANCE(65);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(61);
      END_STATE();
    case 12:
      if (lookahead == '}') ADVANCE(22);
      END_STATE();
    case 13:
      if (lookahead == '}') ADVANCE(24);
      if (lookahead != 0) ADVANCE(8);
      END_STATE();
    case 14:
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(17);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(64);
      END_STATE();
    case 15:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(63);
      END_STATE();
    case 16:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(62);
      END_STATE();
    case 17:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(64);
      END_STATE();
    case 18:
      if (lookahead != 0 &&
          lookahead != '#' &&
          lookahead != '%' &&
          lookahead != '{') ADVANCE(59);
      END_STATE();
    case 19:
      if (eof) ADVANCE(20);
      if (lookahead == '{') ADVANCE(2);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(58);
      if (lookahead != 0) ADVANCE(59);
      END_STATE();
    case 20:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 21:
      ACCEPT_TOKEN(anon_sym_LBRACE_LBRACE);
      END_STATE();
    case 22:
      ACCEPT_TOKEN(anon_sym_RBRACE_RBRACE);
      END_STATE();
    case 23:
      ACCEPT_TOKEN(anon_sym_LBRACE_PERCENT);
      END_STATE();
    case 24:
      ACCEPT_TOKEN(aux_sym_jinja_expression_token1);
      END_STATE();
    case 25:
      ACCEPT_TOKEN(anon_sym_LBRACE_POUND);
      END_STATE();
    case 26:
      ACCEPT_TOKEN(aux_sym__jinja_comment_token1);
      END_STATE();
    case 27:
      ACCEPT_TOKEN(aux_sym__jinja_comment_token1);
      if (lookahead == '#') ADVANCE(6);
      if (lookahead != 0) ADVANCE(5);
      END_STATE();
    case 28:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 29:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 30:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 31:
      ACCEPT_TOKEN(anon_sym_SQUOTE);
      END_STATE();
    case 32:
      ACCEPT_TOKEN(aux_sym_lit_string_token1);
      if (lookahead == '\\') ADVANCE(34);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(32);
      if (lookahead != 0 &&
          lookahead != '\'') ADVANCE(33);
      END_STATE();
    case 33:
      ACCEPT_TOKEN(aux_sym_lit_string_token1);
      if (lookahead == '\\') ADVANCE(34);
      if (lookahead != 0 &&
          lookahead != '\'') ADVANCE(33);
      END_STATE();
    case 34:
      ACCEPT_TOKEN(aux_sym_lit_string_token1);
      if (lookahead != 0 &&
          lookahead != '\\') ADVANCE(33);
      if (lookahead == '\\') ADVANCE(34);
      END_STATE();
    case 35:
      ACCEPT_TOKEN(anon_sym_DQUOTE);
      END_STATE();
    case 36:
      ACCEPT_TOKEN(aux_sym_lit_string_token2);
      if (lookahead == '\\') ADVANCE(38);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(36);
      if (lookahead != 0 &&
          lookahead != '"') ADVANCE(37);
      END_STATE();
    case 37:
      ACCEPT_TOKEN(aux_sym_lit_string_token2);
      if (lookahead == '\\') ADVANCE(38);
      if (lookahead != 0 &&
          lookahead != '"') ADVANCE(37);
      END_STATE();
    case 38:
      ACCEPT_TOKEN(aux_sym_lit_string_token2);
      if (lookahead != 0 &&
          lookahead != '\\') ADVANCE(37);
      if (lookahead == '\\') ADVANCE(38);
      END_STATE();
    case 39:
      ACCEPT_TOKEN(anon_sym_True);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 40:
      ACCEPT_TOKEN(anon_sym_False);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 41:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 42:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 43:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      END_STATE();
    case 44:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      if (lookahead == '#') ADVANCE(25);
      if (lookahead == '%') ADVANCE(23);
      if (lookahead == '{') ADVANCE(21);
      END_STATE();
    case 45:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      END_STATE();
    case 46:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 47:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == '_') ADVANCE(55);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(47);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 48:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 'a') ADVANCE(51);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 49:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 'e') ADVANCE(39);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 50:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 'e') ADVANCE(40);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 51:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 'l') ADVANCE(53);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 52:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 'r') ADVANCE(54);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 53:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 's') ADVANCE(50);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 54:
      ACCEPT_TOKEN(sym__identifier);
      if (lookahead == 'u') ADVANCE(49);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 55:
      ACCEPT_TOKEN(sym__identifier);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(47);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 56:
      ACCEPT_TOKEN(sym__identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(56);
      END_STATE();
    case 57:
      ACCEPT_TOKEN(anon_sym_EQ);
      END_STATE();
    case 58:
      ACCEPT_TOKEN(sym__text);
      if (lookahead == '{') ADVANCE(2);
      if (lookahead == '\t' ||
          lookahead == '\n' ||
          lookahead == '\r' ||
          lookahead == ' ') ADVANCE(58);
      if (lookahead != 0) ADVANCE(59);
      END_STATE();
    case 59:
      ACCEPT_TOKEN(sym__text);
      if (lookahead == '{') ADVANCE(18);
      if (lookahead != 0) ADVANCE(59);
      END_STATE();
    case 60:
      ACCEPT_TOKEN(sym_integer);
      if (lookahead == '.') ADVANCE(65);
      if (lookahead == '_') ADVANCE(10);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(14);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(60);
      END_STATE();
    case 61:
      ACCEPT_TOKEN(sym_integer);
      if (lookahead == '.') ADVANCE(65);
      if (lookahead == '_') ADVANCE(11);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(61);
      END_STATE();
    case 62:
      ACCEPT_TOKEN(sym_integer);
      if (lookahead == '_') ADVANCE(16);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(62);
      END_STATE();
    case 63:
      ACCEPT_TOKEN(sym_float);
      if (lookahead == '_') ADVANCE(65);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(14);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(63);
      END_STATE();
    case 64:
      ACCEPT_TOKEN(sym_float);
      if (lookahead == '_') ADVANCE(66);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(64);
      END_STATE();
    case 65:
      ACCEPT_TOKEN(sym_float);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(14);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(63);
      END_STATE();
    case 66:
      ACCEPT_TOKEN(sym_float);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(64);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 19},
  [2] = {.lex_state = 1},
  [3] = {.lex_state = 1},
  [4] = {.lex_state = 1},
  [5] = {.lex_state = 1},
  [6] = {.lex_state = 1},
  [7] = {.lex_state = 1},
  [8] = {.lex_state = 1},
  [9] = {.lex_state = 1},
  [10] = {.lex_state = 1},
  [11] = {.lex_state = 1},
  [12] = {.lex_state = 1},
  [13] = {.lex_state = 1},
  [14] = {.lex_state = 1},
  [15] = {.lex_state = 1},
  [16] = {.lex_state = 1},
  [17] = {.lex_state = 1},
  [18] = {.lex_state = 1},
  [19] = {.lex_state = 19},
  [20] = {.lex_state = 19},
  [21] = {.lex_state = 0},
  [22] = {.lex_state = 0},
  [23] = {.lex_state = 19},
  [24] = {.lex_state = 0},
  [25] = {.lex_state = 0},
  [26] = {.lex_state = 0},
  [27] = {.lex_state = 19},
  [28] = {.lex_state = 0},
  [29] = {.lex_state = 19},
  [30] = {.lex_state = 1},
  [31] = {.lex_state = 1},
  [32] = {.lex_state = 1},
  [33] = {.lex_state = 1},
  [34] = {.lex_state = 1},
  [35] = {.lex_state = 1},
  [36] = {.lex_state = 1},
  [37] = {.lex_state = 1},
  [38] = {.lex_state = 1},
  [39] = {.lex_state = 1},
  [40] = {.lex_state = 0},
  [41] = {.lex_state = 1},
  [42] = {.lex_state = 1},
  [43] = {.lex_state = 1},
  [44] = {.lex_state = 1},
  [45] = {.lex_state = 1},
  [46] = {.lex_state = 0},
  [47] = {.lex_state = 0},
  [48] = {.lex_state = 0},
  [49] = {.lex_state = 0},
  [50] = {.lex_state = 0},
  [51] = {.lex_state = 0},
  [52] = {.lex_state = 0},
  [53] = {.lex_state = 0},
  [54] = {.lex_state = 0},
  [55] = {.lex_state = 0},
  [56] = {.lex_state = 0},
  [57] = {.lex_state = 0},
  [58] = {.lex_state = 0},
  [59] = {.lex_state = 0},
  [60] = {.lex_state = 0},
  [61] = {.lex_state = 0},
  [62] = {.lex_state = 0},
  [63] = {.lex_state = 0},
  [64] = {.lex_state = 0},
  [65] = {.lex_state = 0},
  [66] = {.lex_state = 0},
  [67] = {.lex_state = 0},
  [68] = {.lex_state = 0},
  [69] = {.lex_state = 0},
  [70] = {.lex_state = 0},
  [71] = {.lex_state = 0},
  [72] = {.lex_state = 0},
  [73] = {.lex_state = 0},
  [74] = {.lex_state = 0},
  [75] = {.lex_state = 0},
  [76] = {.lex_state = 0},
  [77] = {.lex_state = 0},
  [78] = {.lex_state = 0},
  [79] = {.lex_state = 0},
  [80] = {.lex_state = 0},
  [81] = {.lex_state = 0},
  [82] = {.lex_state = 0},
  [83] = {.lex_state = 0},
  [84] = {.lex_state = 0},
  [85] = {.lex_state = 0},
  [86] = {.lex_state = 0},
  [87] = {.lex_state = 0},
  [88] = {.lex_state = 0},
  [89] = {.lex_state = 0},
  [90] = {.lex_state = 0},
  [91] = {.lex_state = 1},
  [92] = {.lex_state = 0},
  [93] = {.lex_state = 0},
  [94] = {.lex_state = 0},
  [95] = {.lex_state = 0},
  [96] = {.lex_state = 7},
  [97] = {.lex_state = 36},
  [98] = {.lex_state = 32},
  [99] = {.lex_state = 0},
  [100] = {.lex_state = 0},
  [101] = {.lex_state = 4},
  [102] = {.lex_state = 32},
  [103] = {.lex_state = 36},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_LBRACE_LBRACE] = ACTIONS(1),
    [anon_sym_LBRACE_PERCENT] = ACTIONS(1),
    [anon_sym_LBRACE_POUND] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [anon_sym_SQUOTE] = ACTIONS(1),
    [anon_sym_DQUOTE] = ACTIONS(1),
    [anon_sym_True] = ACTIONS(1),
    [anon_sym_False] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE] = ACTIONS(1),
    [anon_sym_COLON] = ACTIONS(1),
    [sym__identifier] = ACTIONS(1),
    [anon_sym_EQ] = ACTIONS(1),
    [sym_integer] = ACTIONS(1),
    [sym_float] = ACTIONS(1),
  },
  [1] = {
    [sym_source_file] = STATE(99),
    [sym__jinja_value] = STATE(20),
    [sym_jinja_expression] = STATE(20),
    [sym__jinja_comment] = STATE(20),
    [aux_sym_source_file_repeat1] = STATE(20),
    [ts_builtin_sym_end] = ACTIONS(3),
    [anon_sym_LBRACE_LBRACE] = ACTIONS(5),
    [anon_sym_LBRACE_PERCENT] = ACTIONS(7),
    [anon_sym_LBRACE_POUND] = ACTIONS(9),
    [sym__text] = ACTIONS(11),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 12,
    ACTIONS(13), 1,
      anon_sym_COMMA,
    ACTIONS(15), 1,
      anon_sym_RPAREN,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(29), 1,
      sym_integer,
    ACTIONS(31), 1,
      sym_float,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(55), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [44] = 12,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(33), 1,
      anon_sym_COMMA,
    ACTIONS(35), 1,
      anon_sym_RPAREN,
    ACTIONS(37), 1,
      sym_integer,
    ACTIONS(39), 1,
      sym_float,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(50), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [88] = 12,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(41), 1,
      anon_sym_COMMA,
    ACTIONS(43), 1,
      anon_sym_RBRACK,
    ACTIONS(45), 1,
      sym_integer,
    ACTIONS(47), 1,
      sym_float,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(61), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [131] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(49), 1,
      anon_sym_RPAREN,
    ACTIONS(51), 1,
      sym_integer,
    ACTIONS(53), 1,
      sym_float,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(83), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [172] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(51), 1,
      sym_integer,
    ACTIONS(53), 1,
      sym_float,
    ACTIONS(55), 1,
      anon_sym_RPAREN,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(83), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [213] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(51), 1,
      sym_integer,
    ACTIONS(53), 1,
      sym_float,
    ACTIONS(57), 1,
      anon_sym_RPAREN,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(83), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [254] = 12,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(59), 1,
      anon_sym_COMMA,
    ACTIONS(61), 1,
      anon_sym_RBRACK,
    ACTIONS(63), 1,
      sym_integer,
    ACTIONS(65), 1,
      sym_float,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(60), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [297] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(51), 1,
      sym_integer,
    ACTIONS(53), 1,
      sym_float,
    ACTIONS(67), 1,
      anon_sym_RPAREN,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(83), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [338] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(69), 1,
      anon_sym_RBRACK,
    ACTIONS(71), 1,
      sym_integer,
    ACTIONS(73), 1,
      sym_float,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(75), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [378] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(71), 1,
      sym_integer,
    ACTIONS(73), 1,
      sym_float,
    ACTIONS(75), 1,
      anon_sym_RBRACK,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(75), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [418] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(71), 1,
      sym_integer,
    ACTIONS(73), 1,
      sym_float,
    ACTIONS(77), 1,
      anon_sym_RBRACK,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(75), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [458] = 11,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(71), 1,
      sym_integer,
    ACTIONS(73), 1,
      sym_float,
    ACTIONS(79), 1,
      anon_sym_RBRACK,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(75), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [498] = 10,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(51), 1,
      sym_integer,
    ACTIONS(53), 1,
      sym_float,
    STATE(54), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(83), 7,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
      sym_kwarg,
  [536] = 10,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(71), 1,
      sym_integer,
    ACTIONS(73), 1,
      sym_float,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(75), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [573] = 10,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(81), 1,
      sym_integer,
    ACTIONS(83), 1,
      sym_float,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(91), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [610] = 10,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(85), 1,
      anon_sym_SQUOTE,
    ACTIONS(87), 1,
      anon_sym_DQUOTE,
    ACTIONS(91), 1,
      anon_sym_LBRACK,
    ACTIONS(93), 1,
      anon_sym_LBRACE,
    ACTIONS(95), 1,
      sym_integer,
    ACTIONS(97), 1,
      sym_float,
    STATE(63), 1,
      sym_identifier,
    ACTIONS(89), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(84), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [647] = 10,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(23), 1,
      anon_sym_LBRACK,
    ACTIONS(25), 1,
      anon_sym_LBRACE,
    ACTIONS(27), 1,
      sym__identifier,
    ACTIONS(99), 1,
      sym_integer,
    ACTIONS(101), 1,
      sym_float,
    STATE(82), 1,
      sym_identifier,
    ACTIONS(21), 2,
      anon_sym_True,
      anon_sym_False,
    STATE(64), 6,
      sym__expr,
      sym_fn_call,
      sym_lit_string,
      sym_bool,
      sym_list,
      sym_dict,
  [684] = 6,
    ACTIONS(103), 1,
      ts_builtin_sym_end,
    ACTIONS(105), 1,
      anon_sym_LBRACE_LBRACE,
    ACTIONS(108), 1,
      anon_sym_LBRACE_PERCENT,
    ACTIONS(111), 1,
      anon_sym_LBRACE_POUND,
    ACTIONS(114), 1,
      sym__text,
    STATE(19), 4,
      sym__jinja_value,
      sym_jinja_expression,
      sym__jinja_comment,
      aux_sym_source_file_repeat1,
  [706] = 6,
    ACTIONS(5), 1,
      anon_sym_LBRACE_LBRACE,
    ACTIONS(7), 1,
      anon_sym_LBRACE_PERCENT,
    ACTIONS(9), 1,
      anon_sym_LBRACE_POUND,
    ACTIONS(117), 1,
      ts_builtin_sym_end,
    ACTIONS(119), 1,
      sym__text,
    STATE(19), 4,
      sym__jinja_value,
      sym_jinja_expression,
      sym__jinja_comment,
      aux_sym_source_file_repeat1,
  [728] = 6,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(121), 1,
      anon_sym_COMMA,
    ACTIONS(123), 1,
      anon_sym_RBRACE,
    STATE(46), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [747] = 6,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(125), 1,
      anon_sym_COMMA,
    ACTIONS(127), 1,
      anon_sym_RBRACE,
    STATE(59), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [766] = 2,
    ACTIONS(129), 2,
      ts_builtin_sym_end,
      sym__text,
    ACTIONS(131), 3,
      anon_sym_LBRACE_LBRACE,
      anon_sym_LBRACE_PERCENT,
      anon_sym_LBRACE_POUND,
  [776] = 5,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(133), 1,
      anon_sym_RBRACE,
    STATE(65), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [792] = 5,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(135), 1,
      anon_sym_RBRACE,
    STATE(65), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [808] = 5,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(137), 1,
      anon_sym_RBRACE,
    STATE(65), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [824] = 2,
    ACTIONS(139), 2,
      ts_builtin_sym_end,
      sym__text,
    ACTIONS(141), 3,
      anon_sym_LBRACE_LBRACE,
      anon_sym_LBRACE_PERCENT,
      anon_sym_LBRACE_POUND,
  [834] = 5,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    ACTIONS(143), 1,
      anon_sym_RBRACE,
    STATE(65), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [850] = 2,
    ACTIONS(145), 2,
      ts_builtin_sym_end,
      sym__text,
    ACTIONS(147), 3,
      anon_sym_LBRACE_LBRACE,
      anon_sym_LBRACE_PERCENT,
      anon_sym_LBRACE_POUND,
  [860] = 1,
    ACTIONS(149), 5,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
      anon_sym_COLON,
  [868] = 1,
    ACTIONS(151), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [875] = 1,
    ACTIONS(153), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [882] = 1,
    ACTIONS(155), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [889] = 1,
    ACTIONS(157), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [896] = 1,
    ACTIONS(159), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [903] = 1,
    ACTIONS(161), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [910] = 1,
    ACTIONS(163), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [917] = 1,
    ACTIONS(165), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [924] = 1,
    ACTIONS(167), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [931] = 4,
    ACTIONS(17), 1,
      anon_sym_SQUOTE,
    ACTIONS(19), 1,
      anon_sym_DQUOTE,
    STATE(65), 1,
      sym_pair,
    STATE(92), 1,
      sym_lit_string,
  [944] = 1,
    ACTIONS(169), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [951] = 1,
    ACTIONS(171), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [958] = 1,
    ACTIONS(173), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [965] = 1,
    ACTIONS(175), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [972] = 1,
    ACTIONS(177), 4,
      anon_sym_RBRACE_RBRACE,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
  [979] = 3,
    ACTIONS(179), 1,
      anon_sym_COMMA,
    ACTIONS(181), 1,
      anon_sym_RBRACE,
    STATE(57), 1,
      aux_sym_dict_repeat1,
  [989] = 3,
    ACTIONS(183), 1,
      anon_sym_COMMA,
    ACTIONS(186), 1,
      anon_sym_RPAREN,
    STATE(47), 1,
      aux_sym_argument_list_repeat1,
  [999] = 3,
    ACTIONS(188), 1,
      anon_sym_COMMA,
    ACTIONS(191), 1,
      anon_sym_RBRACK,
    STATE(48), 1,
      aux_sym_list_repeat1,
  [1009] = 3,
    ACTIONS(55), 1,
      anon_sym_RPAREN,
    ACTIONS(193), 1,
      anon_sym_COMMA,
    STATE(47), 1,
      aux_sym_argument_list_repeat1,
  [1019] = 3,
    ACTIONS(195), 1,
      anon_sym_COMMA,
    ACTIONS(197), 1,
      anon_sym_RPAREN,
    STATE(49), 1,
      aux_sym_argument_list_repeat1,
  [1029] = 3,
    ACTIONS(199), 1,
      anon_sym_COMMA,
    ACTIONS(202), 1,
      anon_sym_RBRACE,
    STATE(51), 1,
      aux_sym_dict_repeat1,
  [1039] = 3,
    ACTIONS(133), 1,
      anon_sym_RBRACE,
    ACTIONS(204), 1,
      anon_sym_COMMA,
    STATE(51), 1,
      aux_sym_dict_repeat1,
  [1049] = 3,
    ACTIONS(67), 1,
      anon_sym_RPAREN,
    ACTIONS(206), 1,
      anon_sym_COMMA,
    STATE(47), 1,
      aux_sym_argument_list_repeat1,
  [1059] = 3,
    ACTIONS(208), 1,
      anon_sym_LPAREN,
    ACTIONS(210), 1,
      anon_sym_EQ,
    STATE(36), 1,
      sym_argument_list,
  [1069] = 3,
    ACTIONS(212), 1,
      anon_sym_COMMA,
    ACTIONS(214), 1,
      anon_sym_RPAREN,
    STATE(53), 1,
      aux_sym_argument_list_repeat1,
  [1079] = 3,
    ACTIONS(75), 1,
      anon_sym_RBRACK,
    ACTIONS(216), 1,
      anon_sym_COMMA,
    STATE(48), 1,
      aux_sym_list_repeat1,
  [1089] = 3,
    ACTIONS(143), 1,
      anon_sym_RBRACE,
    ACTIONS(218), 1,
      anon_sym_COMMA,
    STATE(51), 1,
      aux_sym_dict_repeat1,
  [1099] = 3,
    ACTIONS(77), 1,
      anon_sym_RBRACK,
    ACTIONS(220), 1,
      anon_sym_COMMA,
    STATE(48), 1,
      aux_sym_list_repeat1,
  [1109] = 3,
    ACTIONS(222), 1,
      anon_sym_COMMA,
    ACTIONS(224), 1,
      anon_sym_RBRACE,
    STATE(52), 1,
      aux_sym_dict_repeat1,
  [1119] = 3,
    ACTIONS(226), 1,
      anon_sym_COMMA,
    ACTIONS(228), 1,
      anon_sym_RBRACK,
    STATE(56), 1,
      aux_sym_list_repeat1,
  [1129] = 3,
    ACTIONS(230), 1,
      anon_sym_COMMA,
    ACTIONS(232), 1,
      anon_sym_RBRACK,
    STATE(58), 1,
      aux_sym_list_repeat1,
  [1139] = 1,
    ACTIONS(173), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1144] = 2,
    ACTIONS(234), 1,
      anon_sym_LPAREN,
    STATE(69), 1,
      sym_argument_list,
  [1151] = 1,
    ACTIONS(236), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [1156] = 1,
    ACTIONS(202), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1161] = 1,
    ACTIONS(169), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1166] = 1,
    ACTIONS(163), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1171] = 1,
    ACTIONS(175), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1176] = 1,
    ACTIONS(161), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1181] = 1,
    ACTIONS(149), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1186] = 1,
    ACTIONS(153), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1191] = 1,
    ACTIONS(157), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1196] = 1,
    ACTIONS(167), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1201] = 1,
    ACTIONS(171), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1206] = 1,
    ACTIONS(191), 2,
      anon_sym_COMMA,
      anon_sym_RBRACK,
  [1211] = 1,
    ACTIONS(177), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1216] = 1,
    ACTIONS(165), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1221] = 1,
    ACTIONS(155), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1226] = 1,
    ACTIONS(159), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1231] = 1,
    ACTIONS(151), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1236] = 1,
    ACTIONS(238), 2,
      anon_sym_LPAREN,
      anon_sym_EQ,
  [1241] = 2,
    ACTIONS(208), 1,
      anon_sym_LPAREN,
    STATE(36), 1,
      sym_argument_list,
  [1248] = 1,
    ACTIONS(186), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [1253] = 1,
    ACTIONS(240), 2,
      anon_sym_COMMA,
      anon_sym_RBRACE,
  [1258] = 1,
    ACTIONS(242), 1,
      anon_sym_DQUOTE,
  [1262] = 1,
    ACTIONS(228), 1,
      anon_sym_RBRACK,
  [1266] = 1,
    ACTIONS(242), 1,
      anon_sym_SQUOTE,
  [1270] = 1,
    ACTIONS(224), 1,
      anon_sym_RBRACE,
  [1274] = 1,
    ACTIONS(181), 1,
      anon_sym_RBRACE,
  [1278] = 1,
    ACTIONS(244), 1,
      anon_sym_SQUOTE,
  [1282] = 1,
    ACTIONS(246), 1,
      anon_sym_RBRACE_RBRACE,
  [1286] = 1,
    ACTIONS(248), 1,
      anon_sym_COLON,
  [1290] = 1,
    ACTIONS(244), 1,
      anon_sym_DQUOTE,
  [1294] = 1,
    ACTIONS(214), 1,
      anon_sym_RPAREN,
  [1298] = 1,
    ACTIONS(197), 1,
      anon_sym_RPAREN,
  [1302] = 1,
    ACTIONS(250), 1,
      aux_sym_jinja_expression_token1,
  [1306] = 1,
    ACTIONS(252), 1,
      aux_sym_lit_string_token2,
  [1310] = 1,
    ACTIONS(254), 1,
      aux_sym_lit_string_token1,
  [1314] = 1,
    ACTIONS(256), 1,
      ts_builtin_sym_end,
  [1318] = 1,
    ACTIONS(232), 1,
      anon_sym_RBRACK,
  [1322] = 1,
    ACTIONS(258), 1,
      aux_sym__jinja_comment_token1,
  [1326] = 1,
    ACTIONS(260), 1,
      aux_sym_lit_string_token1,
  [1330] = 1,
    ACTIONS(262), 1,
      aux_sym_lit_string_token2,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 44,
  [SMALL_STATE(4)] = 88,
  [SMALL_STATE(5)] = 131,
  [SMALL_STATE(6)] = 172,
  [SMALL_STATE(7)] = 213,
  [SMALL_STATE(8)] = 254,
  [SMALL_STATE(9)] = 297,
  [SMALL_STATE(10)] = 338,
  [SMALL_STATE(11)] = 378,
  [SMALL_STATE(12)] = 418,
  [SMALL_STATE(13)] = 458,
  [SMALL_STATE(14)] = 498,
  [SMALL_STATE(15)] = 536,
  [SMALL_STATE(16)] = 573,
  [SMALL_STATE(17)] = 610,
  [SMALL_STATE(18)] = 647,
  [SMALL_STATE(19)] = 684,
  [SMALL_STATE(20)] = 706,
  [SMALL_STATE(21)] = 728,
  [SMALL_STATE(22)] = 747,
  [SMALL_STATE(23)] = 766,
  [SMALL_STATE(24)] = 776,
  [SMALL_STATE(25)] = 792,
  [SMALL_STATE(26)] = 808,
  [SMALL_STATE(27)] = 824,
  [SMALL_STATE(28)] = 834,
  [SMALL_STATE(29)] = 850,
  [SMALL_STATE(30)] = 860,
  [SMALL_STATE(31)] = 868,
  [SMALL_STATE(32)] = 875,
  [SMALL_STATE(33)] = 882,
  [SMALL_STATE(34)] = 889,
  [SMALL_STATE(35)] = 896,
  [SMALL_STATE(36)] = 903,
  [SMALL_STATE(37)] = 910,
  [SMALL_STATE(38)] = 917,
  [SMALL_STATE(39)] = 924,
  [SMALL_STATE(40)] = 931,
  [SMALL_STATE(41)] = 944,
  [SMALL_STATE(42)] = 951,
  [SMALL_STATE(43)] = 958,
  [SMALL_STATE(44)] = 965,
  [SMALL_STATE(45)] = 972,
  [SMALL_STATE(46)] = 979,
  [SMALL_STATE(47)] = 989,
  [SMALL_STATE(48)] = 999,
  [SMALL_STATE(49)] = 1009,
  [SMALL_STATE(50)] = 1019,
  [SMALL_STATE(51)] = 1029,
  [SMALL_STATE(52)] = 1039,
  [SMALL_STATE(53)] = 1049,
  [SMALL_STATE(54)] = 1059,
  [SMALL_STATE(55)] = 1069,
  [SMALL_STATE(56)] = 1079,
  [SMALL_STATE(57)] = 1089,
  [SMALL_STATE(58)] = 1099,
  [SMALL_STATE(59)] = 1109,
  [SMALL_STATE(60)] = 1119,
  [SMALL_STATE(61)] = 1129,
  [SMALL_STATE(62)] = 1139,
  [SMALL_STATE(63)] = 1144,
  [SMALL_STATE(64)] = 1151,
  [SMALL_STATE(65)] = 1156,
  [SMALL_STATE(66)] = 1161,
  [SMALL_STATE(67)] = 1166,
  [SMALL_STATE(68)] = 1171,
  [SMALL_STATE(69)] = 1176,
  [SMALL_STATE(70)] = 1181,
  [SMALL_STATE(71)] = 1186,
  [SMALL_STATE(72)] = 1191,
  [SMALL_STATE(73)] = 1196,
  [SMALL_STATE(74)] = 1201,
  [SMALL_STATE(75)] = 1206,
  [SMALL_STATE(76)] = 1211,
  [SMALL_STATE(77)] = 1216,
  [SMALL_STATE(78)] = 1221,
  [SMALL_STATE(79)] = 1226,
  [SMALL_STATE(80)] = 1231,
  [SMALL_STATE(81)] = 1236,
  [SMALL_STATE(82)] = 1241,
  [SMALL_STATE(83)] = 1248,
  [SMALL_STATE(84)] = 1253,
  [SMALL_STATE(85)] = 1258,
  [SMALL_STATE(86)] = 1262,
  [SMALL_STATE(87)] = 1266,
  [SMALL_STATE(88)] = 1270,
  [SMALL_STATE(89)] = 1274,
  [SMALL_STATE(90)] = 1278,
  [SMALL_STATE(91)] = 1282,
  [SMALL_STATE(92)] = 1286,
  [SMALL_STATE(93)] = 1290,
  [SMALL_STATE(94)] = 1294,
  [SMALL_STATE(95)] = 1298,
  [SMALL_STATE(96)] = 1302,
  [SMALL_STATE(97)] = 1306,
  [SMALL_STATE(98)] = 1310,
  [SMALL_STATE(99)] = 1314,
  [SMALL_STATE(100)] = 1318,
  [SMALL_STATE(101)] = 1322,
  [SMALL_STATE(102)] = 1326,
  [SMALL_STATE(103)] = 1330,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 0),
  [5] = {.entry = {.count = 1, .reusable = false}}, SHIFT(16),
  [7] = {.entry = {.count = 1, .reusable = false}}, SHIFT(96),
  [9] = {.entry = {.count = 1, .reusable = false}}, SHIFT(101),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(20),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(94),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(39),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(98),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(97),
  [21] = {.entry = {.count = 1, .reusable = false}}, SHIFT(41),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(21),
  [27] = {.entry = {.count = 1, .reusable = false}}, SHIFT(81),
  [29] = {.entry = {.count = 1, .reusable = false}}, SHIFT(55),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(95),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [37] = {.entry = {.count = 1, .reusable = false}}, SHIFT(50),
  [39] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [41] = {.entry = {.count = 1, .reusable = true}}, SHIFT(100),
  [43] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [45] = {.entry = {.count = 1, .reusable = false}}, SHIFT(61),
  [47] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [49] = {.entry = {.count = 1, .reusable = true}}, SHIFT(80),
  [51] = {.entry = {.count = 1, .reusable = false}}, SHIFT(83),
  [53] = {.entry = {.count = 1, .reusable = true}}, SHIFT(83),
  [55] = {.entry = {.count = 1, .reusable = true}}, SHIFT(79),
  [57] = {.entry = {.count = 1, .reusable = true}}, SHIFT(31),
  [59] = {.entry = {.count = 1, .reusable = true}}, SHIFT(86),
  [61] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [63] = {.entry = {.count = 1, .reusable = false}}, SHIFT(60),
  [65] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [67] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [69] = {.entry = {.count = 1, .reusable = true}}, SHIFT(38),
  [71] = {.entry = {.count = 1, .reusable = false}}, SHIFT(75),
  [73] = {.entry = {.count = 1, .reusable = true}}, SHIFT(75),
  [75] = {.entry = {.count = 1, .reusable = true}}, SHIFT(74),
  [77] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [79] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [81] = {.entry = {.count = 1, .reusable = false}}, SHIFT(91),
  [83] = {.entry = {.count = 1, .reusable = true}}, SHIFT(91),
  [85] = {.entry = {.count = 1, .reusable = true}}, SHIFT(102),
  [87] = {.entry = {.count = 1, .reusable = true}}, SHIFT(103),
  [89] = {.entry = {.count = 1, .reusable = false}}, SHIFT(66),
  [91] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [93] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [95] = {.entry = {.count = 1, .reusable = false}}, SHIFT(84),
  [97] = {.entry = {.count = 1, .reusable = true}}, SHIFT(84),
  [99] = {.entry = {.count = 1, .reusable = false}}, SHIFT(64),
  [101] = {.entry = {.count = 1, .reusable = true}}, SHIFT(64),
  [103] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2),
  [105] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(16),
  [108] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(96),
  [111] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(101),
  [114] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(19),
  [117] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1),
  [119] = {.entry = {.count = 1, .reusable = true}}, SHIFT(19),
  [121] = {.entry = {.count = 1, .reusable = true}}, SHIFT(89),
  [123] = {.entry = {.count = 1, .reusable = true}}, SHIFT(44),
  [125] = {.entry = {.count = 1, .reusable = true}}, SHIFT(88),
  [127] = {.entry = {.count = 1, .reusable = true}}, SHIFT(68),
  [129] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__jinja_comment, 2),
  [131] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__jinja_comment, 2),
  [133] = {.entry = {.count = 1, .reusable = true}}, SHIFT(62),
  [135] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [137] = {.entry = {.count = 1, .reusable = true}}, SHIFT(78),
  [139] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__jinja_value, 3),
  [141] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__jinja_value, 3),
  [143] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [145] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_jinja_expression, 2),
  [147] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_jinja_expression, 2),
  [149] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_lit_string, 3),
  [151] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_argument_list, 5),
  [153] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 3),
  [155] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dict, 5),
  [157] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dict, 3),
  [159] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_argument_list, 4),
  [161] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_fn_call, 2, .production_id = 1),
  [163] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 2),
  [165] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 5),
  [167] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_argument_list, 2),
  [169] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_bool, 1),
  [171] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_list, 4),
  [173] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dict, 4),
  [175] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dict, 2),
  [177] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_argument_list, 3),
  [179] = {.entry = {.count = 1, .reusable = true}}, SHIFT(28),
  [181] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [183] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_argument_list_repeat1, 2), SHIFT_REPEAT(14),
  [186] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_argument_list_repeat1, 2),
  [188] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_list_repeat1, 2), SHIFT_REPEAT(15),
  [191] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_list_repeat1, 2),
  [193] = {.entry = {.count = 1, .reusable = true}}, SHIFT(5),
  [195] = {.entry = {.count = 1, .reusable = true}}, SHIFT(6),
  [197] = {.entry = {.count = 1, .reusable = true}}, SHIFT(76),
  [199] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dict_repeat1, 2), SHIFT_REPEAT(40),
  [202] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_dict_repeat1, 2),
  [204] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [206] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [208] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [210] = {.entry = {.count = 1, .reusable = true}}, SHIFT(18),
  [212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(9),
  [214] = {.entry = {.count = 1, .reusable = true}}, SHIFT(45),
  [216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(13),
  [218] = {.entry = {.count = 1, .reusable = true}}, SHIFT(25),
  [220] = {.entry = {.count = 1, .reusable = true}}, SHIFT(10),
  [222] = {.entry = {.count = 1, .reusable = true}}, SHIFT(24),
  [224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(72),
  [226] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(71),
  [230] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [232] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [234] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [236] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_kwarg, 3, .production_id = 2),
  [238] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_identifier, 1),
  [240] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pair, 3, .production_id = 2),
  [242] = {.entry = {.count = 1, .reusable = true}}, SHIFT(70),
  [244] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [246] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(17),
  [250] = {.entry = {.count = 1, .reusable = true}}, SHIFT(29),
  [252] = {.entry = {.count = 1, .reusable = true}}, SHIFT(93),
  [254] = {.entry = {.count = 1, .reusable = true}}, SHIFT(90),
  [256] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [258] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [260] = {.entry = {.count = 1, .reusable = true}}, SHIFT(87),
  [262] = {.entry = {.count = 1, .reusable = true}}, SHIFT(85),
};

#ifdef __cplusplus
extern "C" {
#endif
#ifdef _WIN32
#define extern __declspec(dllexport)
#endif

extern const TSLanguage *tree_sitter_jinja2(void) {
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
