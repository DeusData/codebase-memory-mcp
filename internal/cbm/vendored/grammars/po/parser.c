#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 74
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 40
#define ALIAS_COUNT 0
#define TOKEN_COUNT 21
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 0
#define MAX_ALIAS_SEQUENCE_LENGTH 6
#define PRODUCTION_ID_COUNT 1

enum ts_symbol_identifiers {
  anon_sym_msgctxt = 1,
  anon_sym_msgid = 2,
  anon_sym_LBRACK = 3,
  anon_sym_RBRACK = 4,
  anon_sym_msgid_plural = 5,
  anon_sym_msgstr = 6,
  anon_sym_msgstr_plural = 7,
  anon_sym_POUND = 8,
  anon_sym_POUND_DOT = 9,
  anon_sym_POUND_PIPE = 10,
  anon_sym_POUND_COLON = 11,
  anon_sym_POUND_COMMA = 12,
  anon_sym_POUND_TILDE = 13,
  anon_sym_POUND_TILDE_PIPE = 14,
  sym_number = 15,
  anon_sym_DQUOTE = 16,
  sym_string_fragment = 17,
  aux_sym__escape_sequence_token1 = 18,
  sym_escape_sequence = 19,
  sym_text = 20,
  sym_source_file = 21,
  sym_message = 22,
  sym_msgctxt = 23,
  sym_msgid = 24,
  sym_msgid_plural = 25,
  sym_msgstr = 26,
  sym_msgstr_plural = 27,
  sym_comment = 28,
  sym_translator_comment = 29,
  sym_extracted_comment = 30,
  sym_reference = 31,
  sym_flag = 32,
  sym_previous_untranslated_string = 33,
  sym_string = 34,
  sym__escape_sequence = 35,
  aux_sym_source_file_repeat1 = 36,
  aux_sym_msgid_repeat1 = 37,
  aux_sym_msgstr_repeat1 = 38,
  aux_sym_string_repeat1 = 39,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_msgctxt] = "msgctxt",
  [anon_sym_msgid] = "msgid",
  [anon_sym_LBRACK] = "[",
  [anon_sym_RBRACK] = "]",
  [anon_sym_msgid_plural] = "msgid_plural",
  [anon_sym_msgstr] = "msgstr",
  [anon_sym_msgstr_plural] = "msgstr_plural",
  [anon_sym_POUND] = "#",
  [anon_sym_POUND_DOT] = "#.",
  [anon_sym_POUND_PIPE] = "#|",
  [anon_sym_POUND_COLON] = "#:",
  [anon_sym_POUND_COMMA] = "#,",
  [anon_sym_POUND_TILDE] = "#~",
  [anon_sym_POUND_TILDE_PIPE] = "#~|",
  [sym_number] = "number",
  [anon_sym_DQUOTE] = "\"",
  [sym_string_fragment] = "string_fragment",
  [aux_sym__escape_sequence_token1] = "_escape_sequence_token1",
  [sym_escape_sequence] = "escape_sequence",
  [sym_text] = "text",
  [sym_source_file] = "source_file",
  [sym_message] = "message",
  [sym_msgctxt] = "msgctxt",
  [sym_msgid] = "msgid",
  [sym_msgid_plural] = "msgid_plural",
  [sym_msgstr] = "msgstr",
  [sym_msgstr_plural] = "msgstr_plural",
  [sym_comment] = "comment",
  [sym_translator_comment] = "translator_comment",
  [sym_extracted_comment] = "extracted_comment",
  [sym_reference] = "reference",
  [sym_flag] = "flag",
  [sym_previous_untranslated_string] = "previous_untranslated_string",
  [sym_string] = "string",
  [sym__escape_sequence] = "_escape_sequence",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym_msgid_repeat1] = "msgid_repeat1",
  [aux_sym_msgstr_repeat1] = "msgstr_repeat1",
  [aux_sym_string_repeat1] = "string_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_msgctxt] = anon_sym_msgctxt,
  [anon_sym_msgid] = anon_sym_msgid,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_msgid_plural] = anon_sym_msgid_plural,
  [anon_sym_msgstr] = anon_sym_msgstr,
  [anon_sym_msgstr_plural] = anon_sym_msgstr_plural,
  [anon_sym_POUND] = anon_sym_POUND,
  [anon_sym_POUND_DOT] = anon_sym_POUND_DOT,
  [anon_sym_POUND_PIPE] = anon_sym_POUND_PIPE,
  [anon_sym_POUND_COLON] = anon_sym_POUND_COLON,
  [anon_sym_POUND_COMMA] = anon_sym_POUND_COMMA,
  [anon_sym_POUND_TILDE] = anon_sym_POUND_TILDE,
  [anon_sym_POUND_TILDE_PIPE] = anon_sym_POUND_TILDE_PIPE,
  [sym_number] = sym_number,
  [anon_sym_DQUOTE] = anon_sym_DQUOTE,
  [sym_string_fragment] = sym_string_fragment,
  [aux_sym__escape_sequence_token1] = aux_sym__escape_sequence_token1,
  [sym_escape_sequence] = sym_escape_sequence,
  [sym_text] = sym_text,
  [sym_source_file] = sym_source_file,
  [sym_message] = sym_message,
  [sym_msgctxt] = sym_msgctxt,
  [sym_msgid] = sym_msgid,
  [sym_msgid_plural] = sym_msgid_plural,
  [sym_msgstr] = sym_msgstr,
  [sym_msgstr_plural] = sym_msgstr_plural,
  [sym_comment] = sym_comment,
  [sym_translator_comment] = sym_translator_comment,
  [sym_extracted_comment] = sym_extracted_comment,
  [sym_reference] = sym_reference,
  [sym_flag] = sym_flag,
  [sym_previous_untranslated_string] = sym_previous_untranslated_string,
  [sym_string] = sym_string,
  [sym__escape_sequence] = sym__escape_sequence,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym_msgid_repeat1] = aux_sym_msgid_repeat1,
  [aux_sym_msgstr_repeat1] = aux_sym_msgstr_repeat1,
  [aux_sym_string_repeat1] = aux_sym_string_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_msgctxt] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_msgid] = {
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
  [anon_sym_msgid_plural] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_msgstr] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_msgstr_plural] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_DOT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_COLON] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_TILDE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND_TILDE_PIPE] = {
    .visible = true,
    .named = false,
  },
  [sym_number] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [sym_string_fragment] = {
    .visible = true,
    .named = true,
  },
  [aux_sym__escape_sequence_token1] = {
    .visible = false,
    .named = false,
  },
  [sym_escape_sequence] = {
    .visible = true,
    .named = true,
  },
  [sym_text] = {
    .visible = true,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym_message] = {
    .visible = true,
    .named = true,
  },
  [sym_msgctxt] = {
    .visible = true,
    .named = true,
  },
  [sym_msgid] = {
    .visible = true,
    .named = true,
  },
  [sym_msgid_plural] = {
    .visible = true,
    .named = true,
  },
  [sym_msgstr] = {
    .visible = true,
    .named = true,
  },
  [sym_msgstr_plural] = {
    .visible = true,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_translator_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_extracted_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_reference] = {
    .visible = true,
    .named = true,
  },
  [sym_flag] = {
    .visible = true,
    .named = true,
  },
  [sym_previous_untranslated_string] = {
    .visible = true,
    .named = true,
  },
  [sym_string] = {
    .visible = true,
    .named = true,
  },
  [sym__escape_sequence] = {
    .visible = false,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_msgid_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_msgstr_repeat1] = {
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
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(32);
      if (lookahead == '"') ADVANCE(58);
      if (lookahead == '#') ADVANCE(42);
      if (lookahead == '[') ADVANCE(37);
      if (lookahead == '\\') ADVANCE(21);
      if (lookahead == ']') ADVANCE(38);
      if (lookahead == 'm') ADVANCE(17);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(31);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(56);
      END_STATE();
    case 1:
      if (lookahead == '"') ADVANCE(58);
      if (lookahead == '\\') ADVANCE(21);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(59);
      if (lookahead != 0) ADVANCE(60);
      END_STATE();
    case 2:
      if (lookahead == ',') ADVANCE(2);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(56);
      END_STATE();
    case 3:
      if (lookahead == 'a') ADVANCE(9);
      END_STATE();
    case 4:
      if (lookahead == 'a') ADVANCE(10);
      END_STATE();
    case 5:
      if (lookahead == 'c') ADVANCE(18);
      if (lookahead == 'i') ADVANCE(6);
      if (lookahead == 's') ADVANCE(19);
      END_STATE();
    case 6:
      if (lookahead == 'd') ADVANCE(35);
      END_STATE();
    case 7:
      if (lookahead == 'g') ADVANCE(5);
      END_STATE();
    case 8:
      if (lookahead == 'l') ADVANCE(22);
      END_STATE();
    case 9:
      if (lookahead == 'l') ADVANCE(39);
      END_STATE();
    case 10:
      if (lookahead == 'l') ADVANCE(41);
      END_STATE();
    case 11:
      if (lookahead == 'l') ADVANCE(23);
      END_STATE();
    case 12:
      if (lookahead == 'p') ADVANCE(8);
      END_STATE();
    case 13:
      if (lookahead == 'p') ADVANCE(11);
      END_STATE();
    case 14:
      if (lookahead == 'r') ADVANCE(40);
      END_STATE();
    case 15:
      if (lookahead == 'r') ADVANCE(3);
      END_STATE();
    case 16:
      if (lookahead == 'r') ADVANCE(4);
      END_STATE();
    case 17:
      if (lookahead == 's') ADVANCE(7);
      END_STATE();
    case 18:
      if (lookahead == 't') ADVANCE(24);
      END_STATE();
    case 19:
      if (lookahead == 't') ADVANCE(14);
      END_STATE();
    case 20:
      if (lookahead == 't') ADVANCE(33);
      END_STATE();
    case 21:
      if (lookahead == 'u') ADVANCE(25);
      if (lookahead == 'x') ADVANCE(30);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(62);
      if (lookahead == '"' ||
          lookahead == '\'' ||
          lookahead == '?' ||
          lookahead == '\\' ||
          lookahead == 'a' ||
          lookahead == 'b' ||
          lookahead == 'f' ||
          lookahead == 'n' ||
          lookahead == 'r' ||
          ('t' <= lookahead && lookahead <= 'v')) ADVANCE(63);
      if (lookahead != 0) ADVANCE(61);
      END_STATE();
    case 22:
      if (lookahead == 'u') ADVANCE(15);
      END_STATE();
    case 23:
      if (lookahead == 'u') ADVANCE(16);
      END_STATE();
    case 24:
      if (lookahead == 'x') ADVANCE(20);
      END_STATE();
    case 25:
      if (lookahead == '{') ADVANCE(29);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(27);
      END_STATE();
    case 26:
      if (lookahead == '}') ADVANCE(63);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(26);
      END_STATE();
    case 27:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(30);
      END_STATE();
    case 28:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(63);
      END_STATE();
    case 29:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(26);
      END_STATE();
    case 30:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(28);
      END_STATE();
    case 31:
      if (eof) ADVANCE(32);
      if (lookahead == '"') ADVANCE(58);
      if (lookahead == '#') ADVANCE(42);
      if (lookahead == '[') ADVANCE(37);
      if (lookahead == ']') ADVANCE(38);
      if (lookahead == 'm') ADVANCE(17);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(31);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(56);
      END_STATE();
    case 32:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 33:
      ACCEPT_TOKEN(anon_sym_msgctxt);
      END_STATE();
    case 34:
      ACCEPT_TOKEN(anon_sym_msgctxt);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 35:
      ACCEPT_TOKEN(anon_sym_msgid);
      if (lookahead == '_') ADVANCE(12);
      END_STATE();
    case 36:
      ACCEPT_TOKEN(anon_sym_msgid);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 37:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 38:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 39:
      ACCEPT_TOKEN(anon_sym_msgid_plural);
      END_STATE();
    case 40:
      ACCEPT_TOKEN(anon_sym_msgstr);
      if (lookahead == '_') ADVANCE(13);
      END_STATE();
    case 41:
      ACCEPT_TOKEN(anon_sym_msgstr_plural);
      END_STATE();
    case 42:
      ACCEPT_TOKEN(anon_sym_POUND);
      if (lookahead == ',') ADVANCE(50);
      if (lookahead == '.') ADVANCE(44);
      if (lookahead == ':') ADVANCE(48);
      if (lookahead == '|') ADVANCE(46);
      if (lookahead == '~') ADVANCE(52);
      END_STATE();
    case 43:
      ACCEPT_TOKEN(anon_sym_POUND);
      if (lookahead == ',') ADVANCE(51);
      if (lookahead == '.') ADVANCE(45);
      if (lookahead == ':') ADVANCE(49);
      if (lookahead == '|') ADVANCE(47);
      if (lookahead == '~') ADVANCE(53);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 44:
      ACCEPT_TOKEN(anon_sym_POUND_DOT);
      END_STATE();
    case 45:
      ACCEPT_TOKEN(anon_sym_POUND_DOT);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 46:
      ACCEPT_TOKEN(anon_sym_POUND_PIPE);
      END_STATE();
    case 47:
      ACCEPT_TOKEN(anon_sym_POUND_PIPE);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 48:
      ACCEPT_TOKEN(anon_sym_POUND_COLON);
      END_STATE();
    case 49:
      ACCEPT_TOKEN(anon_sym_POUND_COLON);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 50:
      ACCEPT_TOKEN(anon_sym_POUND_COMMA);
      END_STATE();
    case 51:
      ACCEPT_TOKEN(anon_sym_POUND_COMMA);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 52:
      ACCEPT_TOKEN(anon_sym_POUND_TILDE);
      if (lookahead == '|') ADVANCE(54);
      END_STATE();
    case 53:
      ACCEPT_TOKEN(anon_sym_POUND_TILDE);
      if (lookahead == '|') ADVANCE(55);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 54:
      ACCEPT_TOKEN(anon_sym_POUND_TILDE_PIPE);
      END_STATE();
    case 55:
      ACCEPT_TOKEN(anon_sym_POUND_TILDE_PIPE);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 56:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == ',') ADVANCE(2);
      if (lookahead == '.') ADVANCE(57);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(56);
      END_STATE();
    case 57:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(57);
      END_STATE();
    case 58:
      ACCEPT_TOKEN(anon_sym_DQUOTE);
      END_STATE();
    case 59:
      ACCEPT_TOKEN(sym_string_fragment);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(59);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(60);
      END_STATE();
    case 60:
      ACCEPT_TOKEN(sym_string_fragment);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(60);
      END_STATE();
    case 61:
      ACCEPT_TOKEN(aux_sym__escape_sequence_token1);
      END_STATE();
    case 62:
      ACCEPT_TOKEN(aux_sym__escape_sequence_token1);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(64);
      END_STATE();
    case 63:
      ACCEPT_TOKEN(sym_escape_sequence);
      END_STATE();
    case 64:
      ACCEPT_TOKEN(sym_escape_sequence);
      if (('0' <= lookahead && lookahead <= '7')) ADVANCE(63);
      END_STATE();
    case 65:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == '#') ADVANCE(43);
      if (lookahead == 'm') ADVANCE(69);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(65);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(74);
      END_STATE();
    case 66:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 'c') ADVANCE(70);
      if (lookahead == 'i') ADVANCE(67);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 67:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 'd') ADVANCE(36);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 68:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 'g') ADVANCE(66);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 69:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 's') ADVANCE(68);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 70:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 't') ADVANCE(72);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 71:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 't') ADVANCE(34);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 72:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == 'x') ADVANCE(71);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 73:
      ACCEPT_TOKEN(sym_text);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(73);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(74);
      END_STATE();
    case 74:
      ACCEPT_TOKEN(sym_text);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(74);
      END_STATE();
    case 75:
      ACCEPT_TOKEN(sym_text);
      if (eof) ADVANCE(32);
      if (lookahead == '#') ADVANCE(43);
      if (lookahead == 'm') ADVANCE(69);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(65);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(74);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 0},
  [2] = {.lex_state = 0},
  [3] = {.lex_state = 0},
  [4] = {.lex_state = 0},
  [5] = {.lex_state = 0},
  [6] = {.lex_state = 0},
  [7] = {.lex_state = 0},
  [8] = {.lex_state = 0},
  [9] = {.lex_state = 0},
  [10] = {.lex_state = 0},
  [11] = {.lex_state = 0},
  [12] = {.lex_state = 0},
  [13] = {.lex_state = 0},
  [14] = {.lex_state = 0},
  [15] = {.lex_state = 0},
  [16] = {.lex_state = 0},
  [17] = {.lex_state = 0},
  [18] = {.lex_state = 0},
  [19] = {.lex_state = 0},
  [20] = {.lex_state = 0},
  [21] = {.lex_state = 0},
  [22] = {.lex_state = 75},
  [23] = {.lex_state = 75},
  [24] = {.lex_state = 75},
  [25] = {.lex_state = 0},
  [26] = {.lex_state = 0},
  [27] = {.lex_state = 0},
  [28] = {.lex_state = 0},
  [29] = {.lex_state = 0},
  [30] = {.lex_state = 0},
  [31] = {.lex_state = 0},
  [32] = {.lex_state = 0},
  [33] = {.lex_state = 0},
  [34] = {.lex_state = 0},
  [35] = {.lex_state = 0},
  [36] = {.lex_state = 0},
  [37] = {.lex_state = 0},
  [38] = {.lex_state = 0},
  [39] = {.lex_state = 0},
  [40] = {.lex_state = 0},
  [41] = {.lex_state = 1},
  [42] = {.lex_state = 1},
  [43] = {.lex_state = 0},
  [44] = {.lex_state = 1},
  [45] = {.lex_state = 0},
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
  [71] = {.lex_state = 73},
  [72] = {.lex_state = 0},
  [73] = {.lex_state = 0},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_msgctxt] = ACTIONS(1),
    [anon_sym_msgid] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_msgid_plural] = ACTIONS(1),
    [anon_sym_msgstr] = ACTIONS(1),
    [anon_sym_msgstr_plural] = ACTIONS(1),
    [anon_sym_POUND] = ACTIONS(1),
    [anon_sym_POUND_DOT] = ACTIONS(1),
    [anon_sym_POUND_PIPE] = ACTIONS(1),
    [anon_sym_POUND_COLON] = ACTIONS(1),
    [anon_sym_POUND_COMMA] = ACTIONS(1),
    [anon_sym_POUND_TILDE] = ACTIONS(1),
    [anon_sym_POUND_TILDE_PIPE] = ACTIONS(1),
    [sym_number] = ACTIONS(1),
    [anon_sym_DQUOTE] = ACTIONS(1),
    [aux_sym__escape_sequence_token1] = ACTIONS(1),
    [sym_escape_sequence] = ACTIONS(1),
  },
  [1] = {
    [sym_source_file] = STATE(72),
    [sym_message] = STATE(2),
    [sym_msgctxt] = STATE(54),
    [sym_msgid] = STATE(4),
    [sym_comment] = STATE(2),
    [sym_translator_comment] = STATE(34),
    [sym_extracted_comment] = STATE(34),
    [sym_reference] = STATE(34),
    [sym_flag] = STATE(34),
    [sym_previous_untranslated_string] = STATE(34),
    [aux_sym_source_file_repeat1] = STATE(2),
    [ts_builtin_sym_end] = ACTIONS(3),
    [anon_sym_msgctxt] = ACTIONS(5),
    [anon_sym_msgid] = ACTIONS(7),
    [anon_sym_POUND] = ACTIONS(9),
    [anon_sym_POUND_DOT] = ACTIONS(11),
    [anon_sym_POUND_PIPE] = ACTIONS(13),
    [anon_sym_POUND_COLON] = ACTIONS(15),
    [anon_sym_POUND_COMMA] = ACTIONS(17),
    [anon_sym_POUND_TILDE] = ACTIONS(19),
    [anon_sym_POUND_TILDE_PIPE] = ACTIONS(21),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 14,
    ACTIONS(5), 1,
      anon_sym_msgctxt,
    ACTIONS(7), 1,
      anon_sym_msgid,
    ACTIONS(9), 1,
      anon_sym_POUND,
    ACTIONS(11), 1,
      anon_sym_POUND_DOT,
    ACTIONS(13), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(15), 1,
      anon_sym_POUND_COLON,
    ACTIONS(17), 1,
      anon_sym_POUND_COMMA,
    ACTIONS(19), 1,
      anon_sym_POUND_TILDE,
    ACTIONS(21), 1,
      anon_sym_POUND_TILDE_PIPE,
    ACTIONS(23), 1,
      ts_builtin_sym_end,
    STATE(4), 1,
      sym_msgid,
    STATE(54), 1,
      sym_msgctxt,
    STATE(3), 3,
      sym_message,
      sym_comment,
      aux_sym_source_file_repeat1,
    STATE(34), 5,
      sym_translator_comment,
      sym_extracted_comment,
      sym_reference,
      sym_flag,
      sym_previous_untranslated_string,
  [49] = 14,
    ACTIONS(25), 1,
      ts_builtin_sym_end,
    ACTIONS(27), 1,
      anon_sym_msgctxt,
    ACTIONS(30), 1,
      anon_sym_msgid,
    ACTIONS(33), 1,
      anon_sym_POUND,
    ACTIONS(36), 1,
      anon_sym_POUND_DOT,
    ACTIONS(39), 1,
      anon_sym_POUND_PIPE,
    ACTIONS(42), 1,
      anon_sym_POUND_COLON,
    ACTIONS(45), 1,
      anon_sym_POUND_COMMA,
    ACTIONS(48), 1,
      anon_sym_POUND_TILDE,
    ACTIONS(51), 1,
      anon_sym_POUND_TILDE_PIPE,
    STATE(4), 1,
      sym_msgid,
    STATE(54), 1,
      sym_msgctxt,
    STATE(3), 3,
      sym_message,
      sym_comment,
      aux_sym_source_file_repeat1,
    STATE(34), 5,
      sym_translator_comment,
      sym_extracted_comment,
      sym_reference,
      sym_flag,
      sym_previous_untranslated_string,
  [98] = 9,
    ACTIONS(58), 1,
      anon_sym_msgid_plural,
    ACTIONS(60), 1,
      anon_sym_msgstr,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(9), 1,
      sym_msgid_plural,
    STATE(18), 1,
      aux_sym_msgstr_repeat1,
    STATE(21), 1,
      sym_msgstr,
    STATE(36), 1,
      sym_msgstr_plural,
    ACTIONS(56), 3,
      anon_sym_msgid,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(54), 7,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [134] = 9,
    ACTIONS(58), 1,
      anon_sym_msgid_plural,
    ACTIONS(60), 1,
      anon_sym_msgstr,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(13), 1,
      sym_msgid_plural,
    STATE(18), 1,
      aux_sym_msgstr_repeat1,
    STATE(20), 1,
      sym_msgstr,
    STATE(40), 1,
      sym_msgstr_plural,
    ACTIONS(66), 3,
      anon_sym_msgid,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(64), 7,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [170] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(70), 4,
      anon_sym_msgid,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(68), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid_plural,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [195] = 4,
    ACTIONS(78), 1,
      anon_sym_DQUOTE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(76), 4,
      anon_sym_msgid,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(74), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid_plural,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [220] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(83), 4,
      anon_sym_msgid,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(81), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid_plural,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [245] = 7,
    ACTIONS(60), 1,
      anon_sym_msgstr,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(18), 1,
      aux_sym_msgstr_repeat1,
    STATE(20), 1,
      sym_msgstr,
    STATE(40), 1,
      sym_msgstr_plural,
    ACTIONS(66), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(64), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [275] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(87), 3,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(85), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [299] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(91), 3,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(89), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [323] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(95), 3,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(93), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [347] = 7,
    ACTIONS(60), 1,
      anon_sym_msgstr,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(18), 1,
      aux_sym_msgstr_repeat1,
    STATE(19), 1,
      sym_msgstr,
    STATE(25), 1,
      sym_msgstr_plural,
    ACTIONS(99), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(97), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [377] = 2,
    ACTIONS(103), 4,
      anon_sym_msgid,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(101), 10,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid_plural,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
      anon_sym_DQUOTE,
  [396] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(107), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    STATE(7), 2,
      sym_string,
      aux_sym_msgid_repeat1,
    ACTIONS(105), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [419] = 2,
    ACTIONS(111), 4,
      anon_sym_msgid,
      anon_sym_msgstr,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(109), 10,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid_plural,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
      anon_sym_DQUOTE,
  [438] = 4,
    ACTIONS(115), 1,
      anon_sym_msgstr,
    STATE(17), 1,
      aux_sym_msgstr_repeat1,
    ACTIONS(118), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(113), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [460] = 4,
    ACTIONS(122), 1,
      anon_sym_msgstr,
    STATE(17), 1,
      aux_sym_msgstr_repeat1,
    ACTIONS(124), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(120), 9,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_msgstr_plural,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [482] = 4,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(33), 1,
      sym_msgstr_plural,
    ACTIONS(128), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(126), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [503] = 4,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(25), 1,
      sym_msgstr_plural,
    ACTIONS(99), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(97), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [524] = 4,
    ACTIONS(62), 1,
      anon_sym_msgstr_plural,
    STATE(40), 1,
      sym_msgstr_plural,
    ACTIONS(66), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(64), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [545] = 3,
    ACTIONS(130), 1,
      ts_builtin_sym_end,
    ACTIONS(134), 1,
      sym_text,
    ACTIONS(132), 9,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE,
      anon_sym_POUND_TILDE_PIPE,
  [563] = 3,
    ACTIONS(136), 1,
      ts_builtin_sym_end,
    ACTIONS(140), 1,
      sym_text,
    ACTIONS(138), 9,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE,
      anon_sym_POUND_TILDE_PIPE,
  [581] = 3,
    ACTIONS(142), 1,
      ts_builtin_sym_end,
    ACTIONS(146), 1,
      sym_text,
    ACTIONS(144), 9,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE,
      anon_sym_POUND_TILDE_PIPE,
  [599] = 2,
    ACTIONS(128), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(126), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [614] = 2,
    ACTIONS(150), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(148), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [629] = 2,
    ACTIONS(154), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(152), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [644] = 2,
    ACTIONS(158), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(156), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [659] = 2,
    ACTIONS(162), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(160), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [674] = 2,
    ACTIONS(166), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(164), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [689] = 2,
    ACTIONS(170), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(168), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [704] = 2,
    ACTIONS(174), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(172), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [719] = 2,
    ACTIONS(178), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(176), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [734] = 2,
    ACTIONS(182), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(180), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [749] = 2,
    ACTIONS(186), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(184), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [764] = 2,
    ACTIONS(66), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(64), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [779] = 2,
    ACTIONS(190), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(188), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [794] = 2,
    ACTIONS(194), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(192), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [809] = 2,
    ACTIONS(198), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(196), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [824] = 2,
    ACTIONS(99), 2,
      anon_sym_POUND,
      anon_sym_POUND_TILDE,
    ACTIONS(97), 8,
      ts_builtin_sym_end,
      anon_sym_msgctxt,
      anon_sym_msgid,
      anon_sym_POUND_DOT,
      anon_sym_POUND_PIPE,
      anon_sym_POUND_COLON,
      anon_sym_POUND_COMMA,
      anon_sym_POUND_TILDE_PIPE,
  [839] = 4,
    ACTIONS(200), 1,
      anon_sym_DQUOTE,
    ACTIONS(202), 1,
      sym_string_fragment,
    ACTIONS(204), 2,
      aux_sym__escape_sequence_token1,
      sym_escape_sequence,
    STATE(44), 2,
      sym__escape_sequence,
      aux_sym_string_repeat1,
  [854] = 4,
    ACTIONS(206), 1,
      anon_sym_DQUOTE,
    ACTIONS(208), 1,
      sym_string_fragment,
    ACTIONS(210), 2,
      aux_sym__escape_sequence_token1,
      sym_escape_sequence,
    STATE(41), 2,
      sym__escape_sequence,
      aux_sym_string_repeat1,
  [869] = 5,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(214), 1,
      anon_sym_msgid,
    ACTIONS(216), 1,
      anon_sym_msgstr,
    STATE(32), 1,
      sym_string,
    ACTIONS(212), 2,
      anon_sym_msgctxt,
      anon_sym_msgid_plural,
  [886] = 4,
    ACTIONS(218), 1,
      anon_sym_DQUOTE,
    ACTIONS(220), 1,
      sym_string_fragment,
    ACTIONS(223), 2,
      aux_sym__escape_sequence_token1,
      sym_escape_sequence,
    STATE(44), 2,
      sym__escape_sequence,
      aux_sym_string_repeat1,
  [901] = 3,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(226), 1,
      anon_sym_LBRACK,
    STATE(15), 2,
      sym_string,
      aux_sym_msgid_repeat1,
  [912] = 3,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(228), 1,
      anon_sym_LBRACK,
    STATE(8), 2,
      sym_string,
      aux_sym_msgid_repeat1,
  [923] = 3,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(230), 1,
      anon_sym_LBRACK,
    STATE(12), 2,
      sym_string,
      aux_sym_msgid_repeat1,
  [934] = 4,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(232), 1,
      anon_sym_msgid,
    ACTIONS(234), 1,
      anon_sym_msgid_plural,
    STATE(27), 1,
      sym_string,
  [947] = 3,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(236), 1,
      anon_sym_LBRACK,
    STATE(38), 1,
      sym_string,
  [957] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(10), 2,
      sym_string,
      aux_sym_msgid_repeat1,
  [965] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(11), 2,
      sym_string,
      aux_sym_msgid_repeat1,
  [973] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(6), 2,
      sym_string,
      aux_sym_msgid_repeat1,
  [981] = 3,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    ACTIONS(238), 1,
      anon_sym_LBRACK,
    STATE(37), 1,
      sym_string,
  [991] = 2,
    ACTIONS(7), 1,
      anon_sym_msgid,
    STATE(5), 1,
      sym_msgid,
  [998] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(30), 1,
      sym_string,
  [1005] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(26), 1,
      sym_string,
  [1012] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(38), 1,
      sym_string,
  [1019] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(64), 1,
      sym_string,
  [1026] = 2,
    ACTIONS(72), 1,
      anon_sym_DQUOTE,
    STATE(31), 1,
      sym_string,
  [1033] = 1,
    ACTIONS(240), 1,
      sym_number,
  [1037] = 1,
    ACTIONS(242), 1,
      anon_sym_RBRACK,
  [1041] = 1,
    ACTIONS(244), 1,
      anon_sym_RBRACK,
  [1045] = 1,
    ACTIONS(246), 1,
      anon_sym_RBRACK,
  [1049] = 1,
    ACTIONS(248), 1,
      anon_sym_msgid,
  [1053] = 1,
    ACTIONS(250), 1,
      sym_number,
  [1057] = 1,
    ACTIONS(252), 1,
      anon_sym_RBRACK,
  [1061] = 1,
    ACTIONS(254), 1,
      anon_sym_RBRACK,
  [1065] = 1,
    ACTIONS(226), 1,
      anon_sym_LBRACK,
  [1069] = 1,
    ACTIONS(256), 1,
      sym_number,
  [1073] = 1,
    ACTIONS(258), 1,
      sym_number,
  [1077] = 1,
    ACTIONS(260), 1,
      sym_text,
  [1081] = 1,
    ACTIONS(262), 1,
      ts_builtin_sym_end,
  [1085] = 1,
    ACTIONS(264), 1,
      sym_number,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 49,
  [SMALL_STATE(4)] = 98,
  [SMALL_STATE(5)] = 134,
  [SMALL_STATE(6)] = 170,
  [SMALL_STATE(7)] = 195,
  [SMALL_STATE(8)] = 220,
  [SMALL_STATE(9)] = 245,
  [SMALL_STATE(10)] = 275,
  [SMALL_STATE(11)] = 299,
  [SMALL_STATE(12)] = 323,
  [SMALL_STATE(13)] = 347,
  [SMALL_STATE(14)] = 377,
  [SMALL_STATE(15)] = 396,
  [SMALL_STATE(16)] = 419,
  [SMALL_STATE(17)] = 438,
  [SMALL_STATE(18)] = 460,
  [SMALL_STATE(19)] = 482,
  [SMALL_STATE(20)] = 503,
  [SMALL_STATE(21)] = 524,
  [SMALL_STATE(22)] = 545,
  [SMALL_STATE(23)] = 563,
  [SMALL_STATE(24)] = 581,
  [SMALL_STATE(25)] = 599,
  [SMALL_STATE(26)] = 614,
  [SMALL_STATE(27)] = 629,
  [SMALL_STATE(28)] = 644,
  [SMALL_STATE(29)] = 659,
  [SMALL_STATE(30)] = 674,
  [SMALL_STATE(31)] = 689,
  [SMALL_STATE(32)] = 704,
  [SMALL_STATE(33)] = 719,
  [SMALL_STATE(34)] = 734,
  [SMALL_STATE(35)] = 749,
  [SMALL_STATE(36)] = 764,
  [SMALL_STATE(37)] = 779,
  [SMALL_STATE(38)] = 794,
  [SMALL_STATE(39)] = 809,
  [SMALL_STATE(40)] = 824,
  [SMALL_STATE(41)] = 839,
  [SMALL_STATE(42)] = 854,
  [SMALL_STATE(43)] = 869,
  [SMALL_STATE(44)] = 886,
  [SMALL_STATE(45)] = 901,
  [SMALL_STATE(46)] = 912,
  [SMALL_STATE(47)] = 923,
  [SMALL_STATE(48)] = 934,
  [SMALL_STATE(49)] = 947,
  [SMALL_STATE(50)] = 957,
  [SMALL_STATE(51)] = 965,
  [SMALL_STATE(52)] = 973,
  [SMALL_STATE(53)] = 981,
  [SMALL_STATE(54)] = 991,
  [SMALL_STATE(55)] = 998,
  [SMALL_STATE(56)] = 1005,
  [SMALL_STATE(57)] = 1012,
  [SMALL_STATE(58)] = 1019,
  [SMALL_STATE(59)] = 1026,
  [SMALL_STATE(60)] = 1033,
  [SMALL_STATE(61)] = 1037,
  [SMALL_STATE(62)] = 1041,
  [SMALL_STATE(63)] = 1045,
  [SMALL_STATE(64)] = 1049,
  [SMALL_STATE(65)] = 1053,
  [SMALL_STATE(66)] = 1057,
  [SMALL_STATE(67)] = 1061,
  [SMALL_STATE(68)] = 1065,
  [SMALL_STATE(69)] = 1069,
  [SMALL_STATE(70)] = 1073,
  [SMALL_STATE(71)] = 1077,
  [SMALL_STATE(72)] = 1081,
  [SMALL_STATE(73)] = 1085,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 0, 0, 0),
  [5] = {.entry = {.count = 1, .reusable = true}}, SHIFT(58),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(46),
  [9] = {.entry = {.count = 1, .reusable = false}}, SHIFT(71),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(24),
  [19] = {.entry = {.count = 1, .reusable = false}}, SHIFT(43),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [23] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1, 0, 0),
  [25] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0),
  [27] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(58),
  [30] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(46),
  [33] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(71),
  [36] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(22),
  [39] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(48),
  [42] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(23),
  [45] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(24),
  [48] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(43),
  [51] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(43),
  [54] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message, 1, 0, 0),
  [56] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message, 1, 0, 0),
  [58] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [60] = {.entry = {.count = 1, .reusable = false}}, SHIFT(45),
  [62] = {.entry = {.count = 1, .reusable = true}}, SHIFT(53),
  [64] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message, 2, 0, 0),
  [66] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message, 2, 0, 0),
  [68] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgid, 5, 0, 0),
  [70] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgid, 5, 0, 0),
  [72] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [74] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_msgid_repeat1, 2, 0, 0),
  [76] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_msgid_repeat1, 2, 0, 0),
  [78] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_msgid_repeat1, 2, 0, 0), SHIFT_REPEAT(42),
  [81] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgid, 2, 0, 0),
  [83] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgid, 2, 0, 0),
  [85] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_msgstr_repeat1, 5, 0, 0),
  [87] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_msgstr_repeat1, 5, 0, 0),
  [89] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgid_plural, 5, 0, 0),
  [91] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgid_plural, 5, 0, 0),
  [93] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgid_plural, 2, 0, 0),
  [95] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgid_plural, 2, 0, 0),
  [97] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message, 3, 0, 0),
  [99] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message, 3, 0, 0),
  [101] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 3, 0, 0),
  [103] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 3, 0, 0),
  [105] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgstr, 2, 0, 0),
  [107] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgstr, 2, 0, 0),
  [109] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 2, 0, 0),
  [111] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 2, 0, 0),
  [113] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_msgstr_repeat1, 2, 0, 0),
  [115] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_msgstr_repeat1, 2, 0, 0), SHIFT_REPEAT(68),
  [118] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_msgstr_repeat1, 2, 0, 0),
  [120] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgstr, 1, 0, 0),
  [122] = {.entry = {.count = 1, .reusable = false}}, SHIFT(68),
  [124] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgstr, 1, 0, 0),
  [126] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message, 4, 0, 0),
  [128] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message, 4, 0, 0),
  [130] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_translator_comment, 1, 0, 0),
  [132] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_translator_comment, 1, 0, 0),
  [134] = {.entry = {.count = 1, .reusable = false}}, SHIFT(39),
  [136] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_reference, 1, 0, 0),
  [138] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_reference, 1, 0, 0),
  [140] = {.entry = {.count = 1, .reusable = false}}, SHIFT(28),
  [142] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flag, 1, 0, 0),
  [144] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flag, 1, 0, 0),
  [146] = {.entry = {.count = 1, .reusable = false}}, SHIFT(29),
  [148] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_extracted_comment, 3, 0, 0),
  [150] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_extracted_comment, 3, 0, 0),
  [152] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_extracted_comment, 2, 0, 0),
  [154] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_extracted_comment, 2, 0, 0),
  [156] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_reference, 2, 0, 0),
  [158] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_reference, 2, 0, 0),
  [160] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flag, 2, 0, 0),
  [162] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flag, 2, 0, 0),
  [164] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgstr_plural, 5, 0, 0),
  [166] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgstr_plural, 5, 0, 0),
  [168] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_previous_untranslated_string, 6, 0, 0),
  [170] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_previous_untranslated_string, 6, 0, 0),
  [172] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_previous_untranslated_string, 2, 0, 0),
  [174] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_previous_untranslated_string, 2, 0, 0),
  [176] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_message, 5, 0, 0),
  [178] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_message, 5, 0, 0),
  [180] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comment, 1, 0, 0),
  [182] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comment, 1, 0, 0),
  [184] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comment, 2, 0, 0),
  [186] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comment, 2, 0, 0),
  [188] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgstr_plural, 2, 0, 0),
  [190] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_msgstr_plural, 2, 0, 0),
  [192] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_previous_untranslated_string, 3, 0, 0),
  [194] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_previous_untranslated_string, 3, 0, 0),
  [196] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_translator_comment, 2, 0, 0),
  [198] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_translator_comment, 2, 0, 0),
  [200] = {.entry = {.count = 1, .reusable = false}}, SHIFT(14),
  [202] = {.entry = {.count = 1, .reusable = true}}, SHIFT(44),
  [204] = {.entry = {.count = 1, .reusable = false}}, SHIFT(44),
  [206] = {.entry = {.count = 1, .reusable = false}}, SHIFT(16),
  [208] = {.entry = {.count = 1, .reusable = true}}, SHIFT(41),
  [210] = {.entry = {.count = 1, .reusable = false}}, SHIFT(41),
  [212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(57),
  [214] = {.entry = {.count = 1, .reusable = false}}, SHIFT(57),
  [216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(49),
  [218] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0),
  [220] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0), SHIFT_REPEAT(44),
  [223] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0), SHIFT_REPEAT(44),
  [226] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(65),
  [230] = {.entry = {.count = 1, .reusable = true}}, SHIFT(70),
  [232] = {.entry = {.count = 1, .reusable = false}}, SHIFT(56),
  [234] = {.entry = {.count = 1, .reusable = true}}, SHIFT(56),
  [236] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [238] = {.entry = {.count = 1, .reusable = true}}, SHIFT(69),
  [240] = {.entry = {.count = 1, .reusable = true}}, SHIFT(62),
  [242] = {.entry = {.count = 1, .reusable = true}}, SHIFT(51),
  [244] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [246] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [248] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_msgctxt, 2, 0, 0),
  [250] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [252] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [254] = {.entry = {.count = 1, .reusable = true}}, SHIFT(52),
  [256] = {.entry = {.count = 1, .reusable = true}}, SHIFT(63),
  [258] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [260] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [262] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [264] = {.entry = {.count = 1, .reusable = true}}, SHIFT(66),
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

TS_PUBLIC const TSLanguage *tree_sitter_po(void) {
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
