#include "tree_sitter/parser.h"
#include <string.h>
#include <wctype.h>

enum ObjectScript_Core_Scanner_TokenType {
  _IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE,
  _ASSERT_NO_SPACE_BETWEEN_RULES,
  _ARGUMENTLESS_COMMAND_END,
  _ARGUMENTLESS_LOOP,
  _WHITESPACE,
  TAG,
  ROUTINE,
  ANGLED_BRACKET_FENCED_TEXT,
  PAREN_FENCED_TEXT,
  EMBEDDED_SQL_MARKER,
  EMBEDDED_SQL_REVERSE_MARKER,
  _LINE_COMMENT_INNER,
  _BLOCK_COMMENT_INNER,
  MACRO_VALUE_LINE_WITH_CONTINUE,
  SENTINEL,
  _BOL,
  _TERMINATION,
  _POST_CONDITIONAL_ID,
  _ZW_BLOCK,
  HTML_MARKER,
  HTML_MARKER_REVERSED,
  EMBEDDED_JS_SPECIAL_CASE,
  EMBEDDED_JS_SPECIAL_CASE_COMPLETE,
  POUND_IF_SPECIAL_CASE,
  POUND_IF_SPECIAL_CASE_ELSE,
  POUND_IF_SPECIAL_CASE_ELSE_IF,
  MNEMONIC,
  TAG_END_IF,
  INLINE_COMMENT,
  _STATEMENT_TERMINATION,
  ARGUMENTLESS_INLINE_COMMENT,
  DOTTED_STATEMENT_BLOCK,
  _INTERMEDIATE_TERMINATION,
  BOL_EXTRA,
  _DO_TERMINATION,
  _BOL_BLOCK,
  _POST_CONDITIONAL_END,
  /* Max token type */
  OBJECTSCRIPT_CORE_TOKEN_TYPE_MAX

};

static const char* token_names[] = {
  "_IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE",
  "_ASSERT_NO_SPACE_BETWEEN_RULES",
  "_ARGUMENTLESS_COMMAND_END",
  "_ARGUMENTLESS_LOOP",
  "_WHITESPACE",
  "TAG",
  "ROUTINE",
  "ANGLED_BRACKET_FENCED_TEXT",
  "PAREN_FENCED_TEXT",
  "EMBEDDED_SQL_MARKER",
  "EMBEDDED_SQL_REVERSE_MARKER",
  "_LINE_COMMENT_INNER",
  "_BLOCK_COMMENT_INNER",
  "MACRO_VALUE_LINE_WITH_CONTINUE",
  "SENTINEL",
  "BOL",
  "_INLINE_STATEMENT_SEPARATOR",
  "_TERMINATION",
  "_POST_CONDITIONAL_ID",
  "_ZW_BLOCK",
  "HTML_MARKER",
  "HTML_MARKER_REVERSED",
  "EMBEDDED_JS_SPECIAL_CASE",
  "EMBEDDED_JS_SPECIAL_CASE_COMPLETE",
  "POUND_IF_SPECIAL_CASE",
  "POUND_IF_SPECIAL_CASE_ELSE",
  "POUND_IF_SPECIAL_CASE_ELSE_IF",
  "MNEMONIC",
  "TAG_END_IF",
  "INLINE_COMMENT",
  "_STATEMENT_TERMINATION",
  "ARGUMENTLESS_INLINE_COMMENT",
  "DOTTED_STATEMENT_BLOCK",
  "_INTERMEDIATE_TERMINATION",
  "BOL_EXTRA",
  "_DO_TERMINATION",
  "_BOL_BLOCK",
  "_POST_CONDITIONAL_END"
};

#if 0
static char* debug_enum(TSLexer *lexer, const bool *valid_symbols) {
  static char work[1024];
  size_t n = 0;

  for (int i = 0; i < OBJECTSCRIPT_CORE_TOKEN_TYPE_MAX; i++) {
    if (valid_symbols[i]) {
      if (n > 0) {
        strncpy(&work[n], ", ", sizeof(work)-n);
        n += strlen(&work[n]);
      }
      strncpy(&work[n], token_names[i], sizeof(work)-n);
      n += strlen(&work[n]);
    }
  }

  work[n] = 0;

  return work;
}
#endif

static inline void advance(TSLexer *lexer) {
  lexer->advance(lexer, false);
}
static inline bool is_validHTML_MARKER_char(int32_t c) {
  if (iswspace(c)) return false;

  switch (c) {
    case '<': case '>':
    case '(': case ')':
    case '{': case '}':
    case '+': case '-':
    case '/': case '\\':
    case '|': case '*':
      return false;
    default:
      return true;
  }
}
static inline bool is_valid_sql_marker_char(int32_t c) {
  if (iswspace(c)) return false;

  switch (c) {
    case '(': case ')':
    case '+': case '-':
    case '/': case '\\':
    case '|': case '*':
      return false;
    default:
      return true;
  }
}

static inline bool is_short_circuit_continuation_operator(TSLexer *lexer) {
  if (lexer->lookahead == ',' || lexer->lookahead == '!') {
    lexer->mark_end(lexer);
    return true;
  }
  if (lexer->lookahead != '&' && lexer->lookahead != '|') {
    return false;
  }
  lexer->mark_end(lexer);
  int32_t first = lexer->lookahead;
  advance(lexer);
  return lexer->lookahead == first;
}

static inline void skip   (TSLexer *lexer) { lexer->advance(lexer, true ); }

#define MARKER_BUFFER_MAX_LEN 30
struct ObjectScript_Core_Scanner {
  int32_t marker_buffer[MARKER_BUFFER_MAX_LEN];
  int marker_buffer_len;
  bool terminated_newline;
  // When true, column-1 identifiers are treated as statements unless they
  // are clearly labels/tags.
  bool column1_statement_mode;
  // When true, a column-1 tag named ROUTINE is emitted as ROUTINE instead of TAG.
  bool routine_token_mode;
  bool special_pound_if_mode;
  uint32_t special_pound_if_mode_if_depth;
  int32_t html_marker_buffer[MARKER_BUFFER_MAX_LEN];
  int html_marker_buffer_len;
  int32_t sql_marker_buffer[MARKER_BUFFER_MAX_LEN];
  int sql_marker_buffer_len;
  int32_t js_marker_buffer_reversed[MARKER_BUFFER_MAX_LEN];
  int js_marker_buffer_reversed_len;
  bool is_rtn_dot;
};

static inline bool is_label_char(int32_t c) {
  return iswalnum(c) || c == '%';
}

static inline bool valid_tag_char(int32_t c) {
  return iswalnum(c) || c == '%' || c == '.';
}

static inline int32_t ascii_toupper_i32(int32_t c) {
  if (c >= 'a' && c <= 'z') return c - ('a' - 'A');
  return c;
}

static bool ascii_upper_eq(const int32_t *text, uint32_t len, const char *kw) {
  uint32_t i = 0;
  for (; kw[i] != 0; i++) {
    if (i >= len) return false;
    if (ascii_toupper_i32(text[i]) != (int32_t)kw[i]) return false;
  }
  return i == len;
}

static bool is_statement_or_class_keyword(const int32_t *text, uint32_t len) {
  if (len == 0) return false;
  int32_t c0 = ascii_toupper_i32(text[0]);
  if (!(c0 >= 'A' && c0 <= 'Z')) return false;

  // ZZ* commands
  if (len >= 3 && c0 == 'Z' && ascii_toupper_i32(text[1]) == 'Z') return true;

  // Statement keywords
  if (ascii_upper_eq(text, len, "P") || ascii_upper_eq(text, len, "PRINT")) return true;
  if (ascii_upper_eq(text, len, "ROUTINE")) return true;
  if (ascii_upper_eq(text, len, "ZP") || ascii_upper_eq(text, len, "ZPRINT")) return true;
  if (ascii_upper_eq(text, len, "S") || ascii_upper_eq(text, len, "SET")) return true;
  if (ascii_upper_eq(text, len, "W") || ascii_upper_eq(text, len, "WRITE")) return true;
  if (ascii_upper_eq(text, len, "D") || ascii_upper_eq(text, len, "DO")) return true;
  if (ascii_upper_eq(text, len, "ZW") || ascii_upper_eq(text, len, "ZWRITE")) return true;
  if (ascii_upper_eq(text, len, "F") || ascii_upper_eq(text, len, "FOR")) return true;
  if (ascii_upper_eq(text, len, "WHILE")) return true;
  if (ascii_upper_eq(text, len, "K") || ascii_upper_eq(text, len, "KILL")) return true;
  if (ascii_upper_eq(text, len, "L") || ascii_upper_eq(text, len, "LOCK")) return true;
  if (ascii_upper_eq(text, len, "R") || ascii_upper_eq(text, len, "READ") ||
      ascii_upper_eq(text, len, "RET") || ascii_upper_eq(text, len, "RETURN")) return true;
  if (ascii_upper_eq(text, len, "O") || ascii_upper_eq(text, len, "OPEN")) return true;
  if (ascii_upper_eq(text, len, "CLOSE")) return true;
  if (ascii_upper_eq(text, len, "U") || ascii_upper_eq(text, len, "USE")) return true;
  if (ascii_upper_eq(text, len, "N") || ascii_upper_eq(text, len, "NEW")) return true;
  if (ascii_upper_eq(text, len, "I") || ascii_upper_eq(text, len, "IF")) return true;
  if (ascii_upper_eq(text, len, "E") || ascii_upper_eq(text, len, "ELSE")) return true;
  if (ascii_upper_eq(text, len, "THROW")) return true;
  if (ascii_upper_eq(text, len, "TRY")) return true;
  if (ascii_upper_eq(text, len, "CATCH")) return true;
  if (ascii_upper_eq(text, len, "J") || ascii_upper_eq(text, len, "JOB")) return true;
  if (ascii_upper_eq(text, len, "B") || ascii_upper_eq(text, len, "BREAK")) return true;
  if (ascii_upper_eq(text, len, "M") || ascii_upper_eq(text, len, "MERGE")) return true;
  if (ascii_upper_eq(text, len, "MV")) return true;
  if (ascii_upper_eq(text, len, "MVC") || ascii_upper_eq(text, len, "MVCRT")) return true;
  if (ascii_upper_eq(text, len, "MVDIM")) return true;
  if (ascii_upper_eq(text, len, "MVPRINT")) return true;
  if (ascii_upper_eq(text, len, "Q") || ascii_upper_eq(text, len, "QUIT")) return true;
  if (ascii_upper_eq(text, len, "G") || ascii_upper_eq(text, len, "GOTO")) return true;
  if (ascii_upper_eq(text, len, "H") || ascii_upper_eq(text, len, "HALT") ||
      ascii_upper_eq(text, len, "HANG")) return true;
  if (ascii_upper_eq(text, len, "CONTINUE")) return true;
  if (ascii_upper_eq(text, len, "TC") || ascii_upper_eq(text, len, "TCOMMIT")) return true;
  if (ascii_upper_eq(text, len, "TRO") || ascii_upper_eq(text, len, "TROLLBACK")) return true;
  if (ascii_upper_eq(text, len, "TS") || ascii_upper_eq(text, len, "TSTART")) return true;
  if (ascii_upper_eq(text, len, "X") || ascii_upper_eq(text, len, "XECUTE")) return true;
  if (ascii_upper_eq(text, len, "V") || ascii_upper_eq(text, len, "VIEW")) return true;
  if (ascii_upper_eq(text, len, "ZA") || ascii_upper_eq(text, len, "ZALLOCATE")) return true;
  if (ascii_upper_eq(text, len, "ZB") || ascii_upper_eq(text, len, "ZBREAK")) return true;
  if (ascii_upper_eq(text, len, "ZD") || ascii_upper_eq(text, len, "ZDEALLOCATE")) return true;
  if (ascii_upper_eq(text, len, "ZDELETE")) return true;
  if (ascii_upper_eq(text, len, "ZERASE")) return true;
  if (ascii_upper_eq(text, len, "ZETRAP")) return true;
  if (ascii_upper_eq(text, len, "ZFILE")) return true;
  if (ascii_upper_eq(text, len, "ZGO")) return true;
  if (ascii_upper_eq(text, len, "ZHTRAP")) return true;
  if (ascii_upper_eq(text, len, "ZI") || ascii_upper_eq(text, len, "ZINSERT")) return true;
  if (ascii_upper_eq(text, len, "ZITRAP")) return true;
  if (ascii_upper_eq(text, len, "ZK") || ascii_upper_eq(text, len, "ZKILL")) return true;
  if (ascii_upper_eq(text, len, "ZL") || ascii_upper_eq(text, len, "ZLOAD")) return true;
  if (ascii_upper_eq(text, len, "ZN") || ascii_upper_eq(text, len, "ZNSPACE")) return true;
  if (ascii_upper_eq(text, len, "ZNS")) return true;
  if (ascii_upper_eq(text, len, "ZMOVE")) return true;
  if (ascii_upper_eq(text, len, "ZOMSPACK")) return true;
  if (ascii_upper_eq(text, len, "ZONERROR")) return true;
  if (ascii_upper_eq(text, len, "ZOS")) return true;
  if (ascii_upper_eq(text, len, "ZREAD")) return true;
  if (ascii_upper_eq(text, len, "ZS") || ascii_upper_eq(text, len, "ZSAVE")) return true;
  if (ascii_upper_eq(text, len, "ZSU")) return true;
  if (ascii_upper_eq(text, len, "ZSYNC")) return true;
  if (ascii_upper_eq(text, len, "ZTA")) return true;
  if (ascii_upper_eq(text, len, "ZTB")) return true;
  if (ascii_upper_eq(text, len, "ZTE")) return true;
  if (ascii_upper_eq(text, len, "ZTRANSACTION")) return true;
  if (ascii_upper_eq(text, len, "ZT") || ascii_upper_eq(text, len, "ZTRAP")) return true;
  if (ascii_upper_eq(text, len, "ZU") || ascii_upper_eq(text, len, "ZUSE")) return true;

  // Top-level class/objectscript keywords that can start a non-tag statement.
  if (ascii_upper_eq(text, len, "CLASS")) return true;
  if (ascii_upper_eq(text, len, "METHOD")) return true;
  if (ascii_upper_eq(text, len, "CLASSMETHOD")) return true;
  if (ascii_upper_eq(text, len, "PROPERTY")) return true;
  if (ascii_upper_eq(text, len, "PARAMETER")) return true;
  if (ascii_upper_eq(text, len, "RELATIONSHIP")) return true;
  if (ascii_upper_eq(text, len, "FOREIGNKEY")) return true;
  if (ascii_upper_eq(text, len, "QUERY")) return true;
  if (ascii_upper_eq(text, len, "INDEX")) return true;
  if (ascii_upper_eq(text, len, "TRIGGER")) return true;
  if (ascii_upper_eq(text, len, "XDATA")) return true;
  if (ascii_upper_eq(text, len, "PROJECTION")) return true;
  if (ascii_upper_eq(text, len, "STORAGE")) return true;
  if (ascii_upper_eq(text, len, "IMPORT")) return true;
  if (ascii_upper_eq(text, len, "INCLUDE")) return true;
  if (ascii_upper_eq(text, len, "INCLUDEGENERATOR")) return true;

  return false;
}

static bool ObjectScript_Core_Scanner_lex_fenced_text(
    TSLexer *lexer,
    enum ObjectScript_Core_Scanner_TokenType desired_symbol,
    char l_delim,
    char r_delim) {
  int leftRightDiff = 1;
  while (!lexer->eof(lexer)) {
    if (lexer->lookahead == r_delim) {
      leftRightDiff -= 1;
    } else if (lexer->lookahead == l_delim) {
      leftRightDiff += 1;
    }
    if (leftRightDiff == 0) {
      lexer->result_symbol = desired_symbol;
      return true;
    }
    advance(lexer);
  }
  return false;
}


static bool ObjectScript_Core_Scanner_lex_marker_fenced_text(
    TSLexer *lexer,
    enum ObjectScript_Core_Scanner_TokenType desired_symbol,
    const int32_t *reverse_marker,
    int reverse_marker_len,
    char r_delim
) {
  while (!lexer->eof(lexer)) {
    if (lexer->lookahead == r_delim) {
      lexer->mark_end(lexer);
      advance(lexer);

      uint8_t i = 0;
      while (i < reverse_marker_len && !lexer->eof(lexer)
             && lexer->lookahead == reverse_marker[i]) {
        advance(lexer);
        i++;
      }

      if (i == reverse_marker_len) {
        lexer->result_symbol = desired_symbol;
        return true;
      }

      lexer->mark_end(lexer);
      continue;
    }

    advance(lexer);
  }

  // EOF without closing fence – let parser produce an error
  return false;
}

static inline bool is_ascii_alpha_i32(int32_t c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

// checks if char is part of valid expressions
static inline bool is_objectscript_special_symbol_i32(int32_t c) {
  return c == '+' || c == '-' || c == '\'' || c == '$'|| c == '*' || c == '!' ||
         c == '@' || c == '(' || c == '%' || c == '[' || c == '^' || c == '"' || c == '?' ||
         c == '.' ;
}

static bool ObjectScript_Core_Scanner_lex_pound_if_special_case(TSLexer *lexer) {
  if (lexer->lookahead != '0') return false;

  uint32_t depth = 1;
  bool at_line_start = false;
  advance(lexer);

  while (!lexer->eof(lexer)) {
    if (at_line_start) {
      while (lexer->lookahead == ' ' || lexer->lookahead == '\t') {
        advance(lexer);
      }

      if (lexer->lookahead == '#') {
        lexer->mark_end(lexer);
        advance(lexer);

        int32_t directive[16];
        uint32_t len = 0;
        while (len < (sizeof(directive) / sizeof(directive[0])) &&
               is_ascii_alpha_i32(lexer->lookahead)) {
          directive[len++] = ascii_toupper_i32(lexer->lookahead);
          advance(lexer);
        }

        if (ascii_upper_eq(directive, len, "IF") ||
            ascii_upper_eq(directive, len, "IFDEF") ||
            ascii_upper_eq(directive, len, "IFNDEF") ||
            ascii_upper_eq(directive, len, "IFUNDEF")) {
          depth += 1;
        } else if (ascii_upper_eq(directive, len, "ENDIF")) {
          if (depth == 0) return false;
          depth -= 1;
          if (depth == 0) {

            lexer->result_symbol = POUND_IF_SPECIAL_CASE;
            return true;
          }
        } else if (ascii_upper_eq(directive, len, "ELSE")) {
          if (depth == 0) return false;
          if (depth == 1) {
            lexer->result_symbol = POUND_IF_SPECIAL_CASE_ELSE;
            return true;
          }
        } else if (ascii_upper_eq(directive, len, "ELSEIF")) {
          if (depth == 0) return false;
          if (depth == 1) {
            while (lexer->lookahead== ' ' || lexer->lookahead== '\t') {
              advance(lexer);
            }
            if (lexer->lookahead == '1') {
              depth -= 1;
              lexer->result_symbol = POUND_IF_SPECIAL_CASE_ELSE;
              return true;
            }
            lexer->result_symbol = POUND_IF_SPECIAL_CASE_ELSE_IF;
            return true;
          }
        }


        at_line_start = false;
        continue;
      }

      at_line_start = false;
    }

    if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
      at_line_start = true;
    }

    advance(lexer);
  }

  // No matching #endif found; let grammar/error recovery handle it.
  return false;
}

static bool
ObjectScript_Core_Scanner_scan(struct ObjectScript_Core_Scanner *scanner,
                               TSLexer *lexer, const bool *valid_symbols)
{
#if 0
  if (lexer->log) {
    lexer->log(lexer, "scan: %c (%d): %s\n",
               lexer->lookahead, lexer->lookahead,
               debug_enum(lexer, valid_symbols));
  }
#endif


  // Tree sitter will mark all terminals as valid on error
  // The sentinel should never be valid in a good parse, so this ensures
  // we are not in error recovery mode
  if (valid_symbols[SENTINEL]) {
    return false;
  }
  scanner-> is_rtn_dot = false;

  // this parses any line that ends in ##continue for
  // macro definitions
  if (valid_symbols[MACRO_VALUE_LINE_WITH_CONTINUE] &&
  !lexer->eof(lexer) &&
  lexer->lookahead != '\n') {
    static const char pattern[] = "##continue";
    static const int  len       = sizeof(pattern)-1;
    int pos = 0;
    if (!lexer->eof(lexer) && !iswspace(lexer->lookahead)) {
      scanner->terminated_newline = false;
      return false;
    }
    while (!lexer->eof(lexer) && lexer->lookahead != '\n') {
      char ch = towlower(lexer->lookahead);
      if ((pos < len) && (ch == pattern[pos])) {
        if (pos++ == 0) {
          lexer->mark_end(lexer);
        }

        if (pos == len) {
          advance(lexer);
          int new_line_count = 0;
          while(iswspace(lexer->lookahead) && new_line_count<1) {
            if(lexer->lookahead=='\n') {
              new_line_count++;
            }
            advance(lexer);
          }
          if(new_line_count==1) {
            lexer->mark_end(lexer);
            lexer->result_symbol = MACRO_VALUE_LINE_WITH_CONTINUE;
            return true;
          }
        }
      }
      else {
        if (ch == pattern[0]) {
          pos = 1;
          lexer->mark_end(lexer);
        }
        else {
          pos = 0;
        }
      }
      advance(lexer);
    }
    scanner->terminated_newline = false;
    return false;
  }

  if (valid_symbols[POUND_IF_SPECIAL_CASE]) {
  // #if 0 .. #endif cases
    if (ObjectScript_Core_Scanner_lex_pound_if_special_case(lexer)) {
      if (lexer->result_symbol == POUND_IF_SPECIAL_CASE_ELSE ) {
        scanner->special_pound_if_mode_if_depth += 1;
        scanner->special_pound_if_mode = true;
      }
      scanner->terminated_newline = false;
      return true;
    }
  }

  if (valid_symbols[EMBEDDED_JS_SPECIAL_CASE_COMPLETE]) {
    int i = 0;
    while (i<scanner->js_marker_buffer_reversed_len) {
      advance(lexer);
      i++;
    }
    lexer->result_symbol = EMBEDDED_JS_SPECIAL_CASE_COMPLETE;
    scanner->terminated_newline = false;
    return true;
  }

  if (valid_symbols[EMBEDDED_JS_SPECIAL_CASE]) {
    if (scanner->js_marker_buffer_reversed_len == 0) return false;
    return ObjectScript_Core_Scanner_lex_marker_fenced_text(
      lexer,
      EMBEDDED_JS_SPECIAL_CASE,
      scanner->js_marker_buffer_reversed,
      scanner->js_marker_buffer_reversed_len,
      '>'
    );
  }

  if (valid_symbols[HTML_MARKER_REVERSED]) {
    while (scanner->html_marker_buffer_len >0 && !lexer->eof(lexer)) {
      int32_t expected = scanner->html_marker_buffer[scanner->html_marker_buffer_len - 1];
      if (expected == '[') expected = ']';
      else if (expected == ']') expected = '[';
      if (lexer->lookahead != expected) {
        scanner->terminated_newline = false;
        return false;
      }
      advance(lexer);
      scanner->html_marker_buffer_len -= 1;
    }
    if (scanner->html_marker_buffer_len > 0) {
      return false;
    }
    scanner->html_marker_buffer_len = 0;
    lexer->result_symbol = HTML_MARKER_REVERSED;
    scanner->terminated_newline = false;
    return true;
  }


  if (valid_symbols[HTML_MARKER]) {
    scanner->html_marker_buffer_len=0;
    lexer->mark_end(lexer);
    while (!lexer->eof(lexer) && is_validHTML_MARKER_char(lexer->lookahead)) {
      if (scanner->html_marker_buffer_len == MARKER_BUFFER_MAX_LEN) {
        return false;
      }
      scanner->html_marker_buffer[scanner->html_marker_buffer_len] = lexer->lookahead;
      scanner->html_marker_buffer_len +=1;
      advance(lexer);
      lexer->mark_end(lexer);
    }
    if (scanner->html_marker_buffer_len == 0 || lexer->lookahead != '<') {
      return false;
    }
    scanner->js_marker_buffer_reversed_len = scanner->html_marker_buffer_len;
    for (uint8_t i = 0; i < scanner->html_marker_buffer_len; i++) {
      if (scanner->html_marker_buffer[scanner->html_marker_buffer_len - 1 - i] == '[') {
        scanner->js_marker_buffer_reversed[i] = ']';
      }
      else if (scanner->html_marker_buffer[scanner->html_marker_buffer_len - 1 - i] == ']') {
        scanner->js_marker_buffer_reversed[i] = '[';
      }
      else {
        scanner->js_marker_buffer_reversed[i] = scanner->html_marker_buffer[scanner->html_marker_buffer_len - 1 - i];
      }
    }
    lexer->result_symbol = HTML_MARKER;
    scanner->terminated_newline = false;
    return true;
  }

  if (valid_symbols[EMBEDDED_SQL_REVERSE_MARKER]) {
    while (scanner->sql_marker_buffer_len >0 && !lexer->eof(lexer)) {
      int32_t expected = scanner->sql_marker_buffer[scanner->sql_marker_buffer_len - 1];
      if (expected == '[') expected = ']';
      else if (expected == ']') expected = '[';
      else if (expected == '{') expected = '}';
      else if (expected == '}') expected = '{';
      if (lexer->lookahead != expected) {
        scanner->terminated_newline = false;
        return false;
      }
      advance(lexer);
      scanner->sql_marker_buffer_len -= 1;
    }
    if (scanner->sql_marker_buffer_len > 0) {
      return false;
    }
    scanner->sql_marker_buffer_len = 0;
    lexer->result_symbol = EMBEDDED_SQL_REVERSE_MARKER;
    scanner->terminated_newline = false;
    return true;
  }
  if (valid_symbols[EMBEDDED_SQL_MARKER]) {
    scanner->sql_marker_buffer_len=0;
    lexer->mark_end(lexer);
    while (!lexer->eof(lexer) && is_valid_sql_marker_char(lexer->lookahead)) {
      if (scanner->sql_marker_buffer_len == MARKER_BUFFER_MAX_LEN) {
        return false;
      }
      scanner->sql_marker_buffer[scanner->sql_marker_buffer_len] = lexer->lookahead;
      scanner->sql_marker_buffer_len +=1;
      advance(lexer);
      lexer->mark_end(lexer);
    }
    if (scanner->sql_marker_buffer_len == 0 || lexer->lookahead != '(') {
      return false;
    }
    lexer->result_symbol = EMBEDDED_SQL_MARKER;
    scanner->terminated_newline = false;
    return true;
  }

  if (valid_symbols[_POST_CONDITIONAL_END] && (iswspace(lexer->lookahead) || lexer->eof(lexer) || lexer->lookahead==',' || lexer->lookahead=='}')) {
      lexer->result_symbol = _POST_CONDITIONAL_END;
      scanner->terminated_newline = false;
      return true;
  }

  if (
      valid_symbols[_TERMINATION]
      || valid_symbols[_ARGUMENTLESS_LOOP]
      || valid_symbols[_ARGUMENTLESS_COMMAND_END]
      || valid_symbols[_IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE]
      || valid_symbols[_STATEMENT_TERMINATION]
      || valid_symbols[DOTTED_STATEMENT_BLOCK]
  ) {
    bool valid_symbol_termination = valid_symbols[_TERMINATION];
    bool valid_symbol_intermediate_termination = valid_symbols[_INTERMEDIATE_TERMINATION];
    bool valid_symbol_statement_termination = valid_symbols[_STATEMENT_TERMINATION];
    bool valid_symbol_argumentless_loop = valid_symbols[_ARGUMENTLESS_LOOP];
    bool valid_symbol_argumentless_command_end = valid_symbols[_ARGUMENTLESS_COMMAND_END];
    bool valid_symbol_argumentless_inline_comment = valid_symbols[ARGUMENTLESS_INLINE_COMMENT];
    bool valid_symbol_one_space = valid_symbols[_IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE];
    bool valid_symbol_dotted_statement = valid_symbols[DOTTED_STATEMENT_BLOCK];
    unsigned count = 0;

    while (lexer->lookahead == ' ' || lexer->lookahead == '\t') {
      count ++;
      advance(lexer);
    }

    if (count == 0 && valid_symbol_dotted_statement && lexer->lookahead=='{') {
        lexer->result_symbol = DOTTED_STATEMENT_BLOCK;
        return true;
    }
    // an argument that is exactly one space after the keyword
    if (count == 1 && (iswalnum(lexer->lookahead) || is_objectscript_special_symbol_i32(lexer->lookahead) || lexer->lookahead == '_') && valid_symbol_one_space) {
      lexer->mark_end(lexer);
      lexer->result_symbol = _IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE;
      scanner->terminated_newline = false;
      return true;
    }

    // argumentless command has >= 2 spaces after the keyword
    else if (count >=2 && (iswalnum(lexer->lookahead) || is_objectscript_special_symbol_i32(lexer->lookahead)) && valid_symbol_argumentless_command_end) {
      lexer->mark_end(lexer);
      lexer->result_symbol = _ARGUMENTLESS_COMMAND_END;
      scanner->terminated_newline = false;
      return true;
    }

    else if (count >= 2 && valid_symbol_statement_termination) {
      lexer->mark_end(lexer);
      lexer->result_symbol = _STATEMENT_TERMINATION;
      scanner->terminated_newline = false;
      return true;
    }

    // look for comments #; and ##;
    else if (lexer->lookahead == '#') {
      lexer->mark_end(lexer);
      advance(lexer);
      if (towlower(lexer->lookahead) == 'e' && valid_symbols[TAG] && scanner->special_pound_if_mode && lexer->get_column(lexer) == 1) {
        int32_t directive[16];
        uint32_t len = 0;
        while (len < (sizeof(directive) / sizeof(directive[0])) &&
                is_ascii_alpha_i32(lexer->lookahead)) {
          directive[len++] = ascii_toupper_i32(lexer->lookahead);
          advance(lexer);
        }
        if (ascii_upper_eq(directive, len, "ENDIF")) {
          if (scanner->special_pound_if_mode_if_depth > 0) {
            scanner->special_pound_if_mode_if_depth -= 1;
            if (scanner->special_pound_if_mode_if_depth<1) {
              scanner->special_pound_if_mode=false;
            }
            lexer->result_symbol = TAG_END_IF;
            lexer->mark_end(lexer);
            return true;
          }
          return false;
        }
      }
      if (lexer->lookahead == ';') {
          while (!lexer->eof(lexer) && lexer->lookahead != '\n') {
              advance(lexer);
          }
          lexer->mark_end(lexer);
          lexer->result_symbol = INLINE_COMMENT;
          scanner->terminated_newline = false;
          return true;
      }
      else if (lexer->lookahead == '#') {
          advance(lexer);
          if (lexer->lookahead == ';') {
              while (!lexer->eof(lexer) && lexer->lookahead != '\n') {
                  advance(lexer);
              }
              lexer->mark_end(lexer);
              lexer->result_symbol = INLINE_COMMENT;
              scanner->terminated_newline = false;
              return true;
          }
          else {
              if (valid_symbol_one_space && count == 1) {
                scanner->terminated_newline = false;
                lexer->result_symbol = _IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE;
                return true;
              }
              if (count > 0 && valid_symbols[_WHITESPACE]) {
                scanner->terminated_newline = false;
                lexer->result_symbol = _WHITESPACE;
                return true;
              }
              scanner->terminated_newline = false;
              return false;
          }
      }

      if (valid_symbol_one_space && count == 1) {
        scanner->terminated_newline = false;
        lexer->result_symbol = _IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE;
        return true;
      }
      if (count > 0 && valid_symbols[_WHITESPACE]) {
        scanner->terminated_newline = false;
        lexer->result_symbol = _WHITESPACE;
        return true;
      }
      scanner->terminated_newline = false;
      return false;
    }

    else if (lexer->lookahead == '/') {
      lexer->mark_end(lexer);
      advance(lexer);
      if (is_label_char(lexer->lookahead)) {
          scanner->terminated_newline = false;
          if (valid_symbols[MNEMONIC]) {
            lexer->result_symbol = MNEMONIC;
            return true;
          }

          if (valid_symbols[_IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE] && count == 1) {
            lexer->result_symbol = _IMMEDIATE_SINGLE_WHITESPACE_FOLLOWED_BY_NON_WHITESPACE;
            return true;
          }
      }
      if (lexer->lookahead == '/') {
          while (!lexer->eof(lexer) && lexer->lookahead!= '\n') {
              advance(lexer);
          }
          scanner->terminated_newline = false;
          lexer->mark_end(lexer);
          lexer->result_symbol = INLINE_COMMENT;
          return true;
      }
      else if (lexer->lookahead == '*') {
          if (valid_symbol_statement_termination) {
            lexer->result_symbol = _STATEMENT_TERMINATION;
            scanner->terminated_newline = false;
            return true;
          }
          bool newline = false;
          while (!lexer->eof(lexer)) {
              if (lexer->lookahead == '\n') {
                newline = true;
              }
              if (lexer->lookahead == '*') {
                  advance(lexer);
                  if (lexer->lookahead == '/') {
                      advance(lexer);
                      break;
                  }
              }
              else {
                  advance(lexer);
              }
          }
          // ex: if /* comment */ w 2 is equivalent to if  w 2
          if (valid_symbol_argumentless_inline_comment && !newline) {
            lexer->mark_end(lexer);
            lexer->result_symbol = ARGUMENTLESS_INLINE_COMMENT;
            scanner->terminated_newline = false;
            return true;
          }
          scanner->terminated_newline = false;
          lexer->mark_end(lexer);
          lexer->result_symbol = INLINE_COMMENT;
          return true;
      }
      else {
          return false;
      }
    }

    else if (lexer->lookahead == ';') {
      lexer->mark_end(lexer);
      advance(lexer);
      while (!lexer->eof(lexer) && lexer->lookahead!= '\n') {
          advance(lexer);
      }
      lexer->mark_end(lexer);
      lexer->result_symbol = INLINE_COMMENT;
      scanner->terminated_newline = false;
      return true;
    }

    else if (lexer->lookahead == '\n') {
      lexer->mark_end(lexer);

      // return true early, we don't want to consume
      // the newline because this is for the inner
      // statement (so there is another termination relying on that newline as well)
      if (valid_symbol_statement_termination) {
        lexer->result_symbol = _STATEMENT_TERMINATION;
        scanner->terminated_newline = false;
        return true;
      }

      // for example, terminating an if statement nested in another if statement
      else if (valid_symbol_termination && !valid_symbol_argumentless_loop && !valid_symbol_intermediate_termination && !valid_symbols[_BOL]) {
        lexer->result_symbol = _TERMINATION;
        scanner->terminated_newline = false;
        return true;
      }

      scanner -> terminated_newline = true;
      while (iswspace(lexer->lookahead)) {
          advance(lexer);
      }

      if (valid_symbol_intermediate_termination && valid_symbol_termination && lexer->get_column(lexer) == 0) {
        lexer->mark_end(lexer);
        advance(lexer);
        if (is_label_char(lexer->lookahead)) {
          lexer->result_symbol = _TERMINATION;
          scanner->terminated_newline = false;
          return true;
        }
        else {
          lexer->result_symbol = _WHITESPACE;
          scanner->terminated_newline = false;
          return true;
        }
      }

      else if (lexer->lookahead == '{' && valid_symbol_argumentless_loop) {
        lexer->mark_end(lexer);
        lexer->result_symbol = _ARGUMENTLESS_LOOP;
        scanner->terminated_newline = false;
        return true;
      }

      else if (valid_symbol_intermediate_termination && is_short_circuit_continuation_operator(lexer)) {
        lexer->result_symbol = _WHITESPACE;
        scanner->terminated_newline = false;
        return true;
      }

      else if (valid_symbol_intermediate_termination && lexer->lookahead == '{') {
          lexer->mark_end(lexer);
          lexer->result_symbol = _INTERMEDIATE_TERMINATION;
          scanner->terminated_newline = false;
          return true;
      }

      else if (lexer->lookahead == '#' || lexer->lookahead == ';' || lexer->lookahead == '/') {
          lexer->mark_end(lexer);
          lexer->result_symbol = _WHITESPACE;
          scanner->terminated_newline = false;
          return true;
      }

      // this _TERMINATION token is only for outer _TERMINATION instances
      // so consuming the newline is okay
      else if (valid_symbol_termination && (lexer->lookahead != '.' || !valid_symbols[_BOL])) {
        if (lexer->get_column(lexer) == 0 && is_label_char(lexer->lookahead) && valid_symbols[_BOL]) {
          lexer->mark_end(lexer);
          while(is_label_char(lexer->lookahead) || (iswspace(lexer->lookahead) && lexer->lookahead != '\n')) {
            advance(lexer);
          }
          if (lexer->lookahead == '.') {
            lexer->result_symbol = _WHITESPACE;
            return true;
          }
          else {
            lexer->result_symbol = _TERMINATION;
            return true;
          }
        }
        scanner->terminated_newline = false;
        lexer->result_symbol = _TERMINATION;
        return true;
      }

      else {
        lexer->mark_end(lexer);
        lexer->result_symbol = _WHITESPACE;
        return true;
      }
    }

    else if (lexer->lookahead == '{') {
      // this token is for commands that allow blocks as part of their command ex: w {"hi": "bye"}
      if (count == 1 && valid_symbols[_ZW_BLOCK]) {
        lexer->result_symbol = _ZW_BLOCK;
        scanner->terminated_newline = false;
        return true;
      }

      if (valid_symbols[DOTTED_STATEMENT_BLOCK]) {
        lexer->mark_end(lexer);
        lexer->result_symbol = DOTTED_STATEMENT_BLOCK;
      }

      else if (valid_symbol_argumentless_loop) {
        lexer->mark_end(lexer);
        lexer->result_symbol = _ARGUMENTLESS_LOOP;
        scanner->terminated_newline = false;
        return true;
      }
    }

    if (count > 0 && valid_symbols[_WHITESPACE]) {

      // there can't be a space between a keyword and the post-conditional,
      // this should be an error, returning false will result in that
      if (valid_symbols[_POST_CONDITIONAL_ID] && lexer->lookahead ==':') {
        scanner->terminated_newline = false;
        return false;
      }
      lexer->mark_end(lexer);
      lexer->result_symbol = _WHITESPACE;
      return true;
    }

    if (lexer->eof(lexer) || lexer-> lookahead == '}') {
        if (valid_symbol_statement_termination) {
          lexer->result_symbol = _STATEMENT_TERMINATION;
          scanner->terminated_newline = false;
          return true;
        }
        else if (valid_symbols[_DO_TERMINATION] &&  lexer-> lookahead == '}') {
            lexer->result_symbol = _DO_TERMINATION;
            scanner->terminated_newline = false;
            return true;
        }
        lexer->result_symbol = _TERMINATION;
        scanner->terminated_newline = false;
        return true;
    }
  }

  if (scanner->terminated_newline && valid_symbols[_BOL]) {
      lexer->mark_end(lexer);
      // rtn dot is a single dot on a line (represents a blank line)
      if (scanner->routine_token_mode && lexer->get_column(lexer) == 0 && lexer->lookahead == '.') {
        advance(lexer);
        while (iswspace(lexer->lookahead)) {
            if (lexer->lookahead == '\n') {
                // fallback to objectscript_routine scanner.c logic
                lexer->mark_end(lexer);
                scanner->is_rtn_dot = true;
                return false;
            }
            advance(lexer);
        }
      }
      while (iswspace(lexer->lookahead) && lexer->lookahead != '\n') {
          advance(lexer);
      }
      if (lexer->lookahead == '.') {
          while (lexer->lookahead == '.' || (iswspace(lexer->lookahead) && lexer->lookahead != '\n')) {
              advance(lexer);
          }
          if (lexer->lookahead == '}') {
              lexer->result_symbol = _BOL_BLOCK;
              scanner->terminated_newline = false;
              return true;
          }
          lexer->result_symbol = _BOL;
          scanner->terminated_newline = false;
          return true;
      }
      else if (lexer->get_column(lexer) == 0 && is_label_char(lexer->lookahead)) {
        while (valid_tag_char(lexer->lookahead)) advance(lexer);
        while (lexer->lookahead == ' ' || lexer->lookahead == '\t') advance(lexer);

        if (lexer->lookahead == '.') {
          lexer->result_symbol = _BOL;
          scanner->terminated_newline = false;
          return true;
        }
        else {
          lexer->mark_end(lexer);
          lexer->result_symbol = TAG;
          scanner->terminated_newline = false;
          return true;
        }
      }
  }

  if (valid_symbols[_POST_CONDITIONAL_ID] && lexer->lookahead==':') {
    lexer->mark_end(lexer);
    lexer->advance(lexer, false);
    if (!(iswspace(lexer->lookahead))) {
      lexer->result_symbol = _POST_CONDITIONAL_ID;
      scanner->terminated_newline = false;
      return true;
    }
  }
  else if (valid_symbols[_ASSERT_NO_SPACE_BETWEEN_RULES]) {
    if (!iswspace(lexer->lookahead)) {
      lexer->result_symbol = _ASSERT_NO_SPACE_BETWEEN_RULES;
      scanner->terminated_newline = false;
      return true;
    }
    return false;
  }
  else if
      (
      valid_symbols[TAG] &&
      lexer->get_column(lexer) == 0 &&
      is_label_char(lexer->lookahead)
      )
  {
    int32_t ident[96];
    uint32_t len = 0;
    do {
      if (len < sizeof(ident) / sizeof(ident[0])) ident[len++] = lexer->lookahead;
      advance(lexer);
    } while (valid_tag_char(lexer->lookahead));

    if (scanner->routine_token_mode &&
        valid_symbols[ROUTINE] &&
        ascii_upper_eq(ident, len, "ROUTINE")) {
      lexer->result_symbol = ROUTINE;
      scanner->terminated_newline = false;
      lexer->mark_end(lexer);
      return true;
    }

    if (!scanner->column1_statement_mode) {
      lexer->result_symbol = TAG;
      scanner->terminated_newline = false;
      lexer->mark_end(lexer);
      return true;
    }
    // Rule 1: if there is a tab after the identifier, treat as a definite tag.
    if (lexer->lookahead == '\t') {
      lexer->result_symbol = TAG;
      scanner->terminated_newline = false;
      lexer->mark_end(lexer);
      return true;
    }
    // Rule 2/3: in column-1 statement mode, keyword-like names default to
    // statements/class-statements; non-keywords are tags.
    if (!is_statement_or_class_keyword(ident, len)) {
      lexer->result_symbol = TAG;
      scanner->terminated_newline = false;
      lexer->mark_end(lexer);
      return true;
    }
    return false;
  }
  else if (valid_symbols[ANGLED_BRACKET_FENCED_TEXT]) {
    bool ok = ObjectScript_Core_Scanner_lex_fenced_text(
        lexer, ANGLED_BRACKET_FENCED_TEXT, '<', '>');
    return ok;
  }
  else if (valid_symbols[PAREN_FENCED_TEXT]) {
    bool ok = ObjectScript_Core_Scanner_lex_fenced_text(
        lexer, PAREN_FENCED_TEXT, '(', ')');
    return ok;
  }

  else if (valid_symbols[_LINE_COMMENT_INNER]) {
    lexer->result_symbol = _LINE_COMMENT_INNER;
    for (;;) {
      if (lexer->eof(lexer)) {
        scanner->terminated_newline = false;
        return true;
      }

      if (lexer->lookahead == '\n') {
        scanner->terminated_newline = false;
        return true;
      }

      advance(lexer);
    }
  }
  else if (valid_symbols[_BLOCK_COMMENT_INNER]) {
    while (!lexer->eof(lexer)) {
      if (lexer->lookahead == '*') {
        lexer->mark_end(lexer);
        advance(lexer);
        if (lexer->lookahead == '/') {
          lexer->result_symbol = _BLOCK_COMMENT_INNER;
          scanner->terminated_newline = false;
          return true;
        }
      }
      else {
        advance(lexer);
        lexer->mark_end(lexer);
      }
    }
  }
  else if (valid_symbols[_WHITESPACE] && (iswspace(lexer->lookahead)))  {
    bool consumed = false;
    while (iswspace(lexer->lookahead)) {
      if (lexer->lookahead == '\n') {
        // End the current statement before the next dotted line so
        // constructs like `x` newline `. quit` do not collapse into `x.quit`.
        if (valid_symbols[_TERMINATION] && consumed) {
          lexer->mark_end(lexer);
          lexer->result_symbol = _WHITESPACE;
          return true;
        }
        scanner->terminated_newline = true;
      }
      advance(lexer);
      consumed = true;
    }
    if (!consumed) return false;
    bool starts_line_sensitive_construct =
        lexer->lookahead == '.' ||
        (lexer->get_column(lexer) == 0 && is_label_char(lexer->lookahead));
    bool line_sensitive_context =
        valid_symbols[_BOL] ||
        valid_symbols[TAG] ||
        valid_symbols[_TERMINATION] ||
        valid_symbols[_STATEMENT_TERMINATION] ||
        valid_symbols[_ARGUMENTLESS_LOOP] ||
        valid_symbols[_ARGUMENTLESS_COMMAND_END] ||
        valid_symbols[DOTTED_STATEMENT_BLOCK] ||
        valid_symbols[_INTERMEDIATE_TERMINATION];
    if (!starts_line_sensitive_construct || !line_sensitive_context) {
      scanner->terminated_newline = false;
    }

    lexer->mark_end(lexer);
    lexer->result_symbol = _WHITESPACE;
    return true;
  }
  else if (valid_symbols[TAG] && scanner->special_pound_if_mode) {
    while (iswspace(lexer->lookahead)) {
      advance(lexer);
    }
    if (lexer->lookahead == '#') {
      lexer->mark_end(lexer);
      advance(lexer);
      int32_t directive[16];
      uint32_t len = 0;
      while (len < (sizeof(directive) / sizeof(directive[0])) &&
              is_ascii_alpha_i32(lexer->lookahead)) {
        directive[len++] = ascii_toupper_i32(lexer->lookahead);
        advance(lexer);
      }
      if (ascii_upper_eq(directive, len, "ENDIF")) {
        if (scanner->special_pound_if_mode_if_depth > 0) {
          scanner->special_pound_if_mode_if_depth -= 1;
          if (scanner->special_pound_if_mode_if_depth<1) {
            scanner->special_pound_if_mode=false;
          }
          lexer->result_symbol = TAG_END_IF;
          lexer->mark_end(lexer);
          return true;
        }
        return false;
      }
    }
  }

  if (scanner->terminated_newline && lexer->lookahead == '.' && !valid_symbols[_BOL] && !valid_symbols[_INTERMEDIATE_TERMINATION] && !valid_symbols[_BOL_BLOCK]) {
    while (lexer->lookahead == '.') {
      advance(lexer);
    }
    lexer->mark_end(lexer);
    lexer->result_symbol = BOL_EXTRA;
    scanner->terminated_newline = false;
    return true;
  }
  scanner->terminated_newline = false;
  return false;
}

static void ObjectScript_Core_Scanner_init(struct ObjectScript_Core_Scanner *scanner) {
  scanner->sql_marker_buffer_len = 0;
  scanner->html_marker_buffer_len = 0;
  scanner->terminated_newline = false;
  scanner->column1_statement_mode = false;
  scanner->routine_token_mode = false;
  scanner->special_pound_if_mode = false;
  scanner->special_pound_if_mode_if_depth = 0;
  scanner->is_rtn_dot = false;
}
