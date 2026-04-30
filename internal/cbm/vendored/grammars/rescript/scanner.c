#include "tree_sitter/parser.h"
#include <string.h>
#include <wctype.h>

enum TokenType {
  NEWLINE,
  COMMENT,
  NEWLINE_AND_COMMENT,
  QUOTE,
  BACKTICK,
  TEMPLATE_CHARS,
  L_PAREN,
  R_PAREN,
  LIST_CONSTRUCTOR,
  DICT_CONSTRUCTOR,
  DECORATOR,
  DECORATOR_INLINE,
};

typedef struct ScannerState {
  int parens_nesting;
  bool in_quotes;
  bool in_backticks;
  bool eof_reported;
} ScannerState;

void *tree_sitter_rescript_external_scanner_create() {
  void* state = malloc(sizeof(ScannerState));
  memset(state, 0, sizeof(ScannerState));
  return state;
}

void tree_sitter_rescript_external_scanner_destroy(void* state) {
  free(state);
}

void tree_sitter_rescript_external_scanner_reset(void* state) {
  memset(state, 0, sizeof(ScannerState));
}

unsigned tree_sitter_rescript_external_scanner_serialize(void* state, char *buffer) {
  memcpy(buffer, state, sizeof(ScannerState));
  return sizeof(ScannerState);
}

void tree_sitter_rescript_external_scanner_deserialize(void* state, const char *buffer, unsigned n_bytes) {
  memcpy(state, buffer, n_bytes);
}

static void advance(TSLexer *lexer) { lexer->advance(lexer, false); }
static void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static bool is_inline_whitespace(int32_t c) {
  return c == ' ' || c == '\t';
}

static bool is_identifier_start(char c) {
  return c == '_' || (c >= 'a' && c <= 'z');
}

static bool is_decorator_start(char c) {
  return c == '_' || c == '\\' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static bool is_decorator_identifier(char c) {
  return c == '_' || c == '.' || c == '\'' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

static bool is_whitespace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

static void scan_whitespace(TSLexer *lexer, bool skip) {
  while (iswspace(lexer->lookahead) && !lexer->eof(lexer)) {
    lexer->advance(lexer, skip);
  }
}

static void scan_multiline_comment(TSLexer *lexer) {
  int level = 1;
  advance(lexer);
  while (level > 0 && !lexer->eof(lexer)) {
    switch (lexer->lookahead) {
      case '/':
        advance(lexer);
        if (lexer->lookahead == '*')
          ++level;
        else
          continue;

        break;

      case '*':
        advance(lexer);
        if (lexer->lookahead == '/')
          --level;
        else
          continue;

        break;
    }

    advance(lexer);
  }
}

static bool scan_comment(TSLexer *lexer) {
  if (lexer->lookahead != '/')
    return false;

  advance(lexer);
  switch (lexer->lookahead) {
    case '/':
      // Single-line comment
      do {
        advance(lexer);
      } while (lexer->lookahead != '\n' && !lexer->eof(lexer));
      return true;

    case '*':
      // Multi-line comment
      scan_multiline_comment(lexer);
      return true;

    default:
      // Division, etc
      return false;
  }
}

static bool scan_whitespace_and_comments(TSLexer *lexer) {
  bool has_comments = false;
  while (!lexer->eof(lexer)) {
    // Once a comment is found, the subsequent whitespace should not be marked
    // as skipped to keep the correct range of the comment node if it will be
    // marked so.
    bool skip_whitespace = !has_comments;
    scan_whitespace(lexer, skip_whitespace);
    if (scan_comment(lexer)) {
      has_comments = true;
    } else {
      break;
    }
  }

  return has_comments;
}

bool tree_sitter_rescript_external_scanner_scan(
    void* payload,
    TSLexer* lexer,
    const bool* valid_symbols
    ) {
  ScannerState* state = (ScannerState*)payload;
  bool in_string = state->in_quotes || state->in_backticks;

  while (is_inline_whitespace(lexer->lookahead) && !in_string) {
    skip(lexer);
  }

  if (valid_symbols[TEMPLATE_CHARS]) {
    lexer->result_symbol = TEMPLATE_CHARS;
    for (bool has_content = false;; has_content = true) {
      lexer->mark_end(lexer);
      switch (lexer->lookahead) {
        case '`':
          state->in_backticks = false;
          return has_content;
        case '\0':
          return false;
        case '$':
          advance(lexer);
          if (lexer->lookahead == '{' || is_identifier_start(lexer->lookahead)) {
            return has_content;
          }
          break;
        case '\\':
          return has_content;
        default:
          advance(lexer);
      }
    }

    return true;
  }

  // If a source file missing EOL at EOF, give the last statement a chance:
  // report the statement delimiting EOL at the very end of file. Make sure
  // it’s done only once, otherwise the scanner will enter dead-lock because
  // we report NEWLINE again and again, no matter the lexer is exhausted
  // already.
  if (valid_symbols[NEWLINE] && lexer->eof(lexer) && !state->eof_reported) {
    lexer->result_symbol = NEWLINE;
    state->eof_reported = true;
    return true;
  }

  // Magic ahead!
  // We have two types of newline in ReScript. The one which ends the current statement,
  // and the one used just for pretty-formatting (e.g. separates variant type values).
  // We report only the first one. The second one should be ignored and skipped as
  // whitespace.
  // What makes things worse is that we can have comments interleaved in statements.
  // Tree-sitter gives just one chance to say what type of a token we’re on. We can’t
  // say: “I see a significant newline, then I see a comment”. To deal with it, an
  // artificial token NEWLINE_AND_COMMENT was introduced. It has the same semantics for
  // the AST as simple newline and the same highlighting as a usual comment.
  if (valid_symbols[NEWLINE] && lexer->lookahead == '\n') {
    bool is_unnested = state->parens_nesting == 0;
    lexer->result_symbol = NEWLINE;
    lexer->advance(lexer, true);
    lexer->mark_end(lexer);

    bool has_comment = scan_whitespace_and_comments(lexer);
    if (has_comment && valid_symbols[NEWLINE_AND_COMMENT]) {
      lexer->result_symbol = NEWLINE_AND_COMMENT;
      lexer->mark_end(lexer);
    }

    bool in_multiline_statement = false;
    if (lexer->lookahead == '-') {
      advance(lexer);
      if (lexer->lookahead == '>') {
        // Ignore new lines before pipe operator (->)
        in_multiline_statement = true;
      }
    } else if (lexer->lookahead == '|') {
      // Ignore new lines before variant declarations and switch matches
      in_multiline_statement = true;
    } else if (lexer->lookahead == '?' || lexer->lookahead == ':') {
      // Ignore new lines before potential ternaries
      in_multiline_statement = true;
    } else if (lexer->lookahead == '}') {
      // Do not report new lines right before block/switch closings to avoid
      // parser confustion between a terminated and unterminated statements
      // for rules like seq(repeat($._statement), $.statement)
      in_multiline_statement = true;
    } else if (lexer->lookahead == 'a') {
      advance(lexer);
      if (lexer->lookahead == 'n') {
        advance(lexer);
        if (lexer->lookahead == 'd') {
          // Ignore new lines before `and` keyword (recursive definition)
          in_multiline_statement = true;
        }
      }
    } else if (lexer->lookahead == 'e') {
      advance(lexer);
      if (lexer->lookahead == 'l') {
        advance(lexer);
        if (lexer->lookahead == 's') {
          advance(lexer);
          if (lexer->lookahead == 'e') {
            // Ignore new lines before `else` keyword (else/else if clauses)
            in_multiline_statement = true;
          }
        }
      }
    } else if (lexer->lookahead == 'w') {
      advance(lexer);
      if (lexer->lookahead == 'i') {
        advance(lexer);
        if (lexer->lookahead == 't') {
          advance(lexer);
          if (lexer->lookahead == 'h') {
            // Ignore new lines before `with` keyword (module type constraints)
            in_multiline_statement = true;
          }
        }
      }
    }

    if (in_multiline_statement) {
      if (has_comment && valid_symbols[COMMENT]) {
        lexer->result_symbol = COMMENT;
        return true;
      }
    } else {
      return true;
    }
  }

  if (!in_string) {
    scan_whitespace(lexer, true);
  }

  if (valid_symbols[COMMENT] && lexer->lookahead == '/' && !in_string) {
    lexer->result_symbol = COMMENT;
    if (scan_comment(lexer)) {
      lexer->mark_end(lexer);
      return true;
    } else {
      return false;
    }
  }

  if (valid_symbols[QUOTE] && lexer->lookahead == '"') {
    state->in_quotes = !state->in_quotes;
    lexer->result_symbol = QUOTE;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
  }

  if (valid_symbols[BACKTICK] && lexer->lookahead == '`') {
    state->in_backticks = !state->in_backticks;
    lexer->result_symbol = BACKTICK;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
  }

  if (valid_symbols[L_PAREN] && lexer->lookahead == '(') {
    ++state->parens_nesting;
    lexer->result_symbol = L_PAREN;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
  }

  if (valid_symbols[R_PAREN] && lexer->lookahead == ')') {
    --state->parens_nesting;
    lexer->result_symbol = R_PAREN;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
  }

  if (valid_symbols[LIST_CONSTRUCTOR]) {
    lexer->result_symbol = LIST_CONSTRUCTOR;
    if (lexer->lookahead == 'l') {
      advance(lexer);
      if (lexer->lookahead == 'i') {
        advance(lexer);
        if (lexer->lookahead == 's') {
          advance(lexer);
          if (lexer->lookahead == 't') {
            advance(lexer);
            if (lexer->lookahead == '{') {
              lexer->mark_end(lexer);
              return true;
            }
          }
        }
      }
    }
  }

  if (valid_symbols[DICT_CONSTRUCTOR]) {
    lexer->result_symbol = DICT_CONSTRUCTOR;
    if (lexer->lookahead == 'd') {
      advance(lexer);
      if (lexer->lookahead == 'i') {
        advance(lexer);
        if (lexer->lookahead == 'c') {
          advance(lexer);
          if (lexer->lookahead == 't') {
            advance(lexer);
            if (lexer->lookahead == '{') {
              lexer->mark_end(lexer);
              return true;
            }
          }
        }
      }
    }
  }

  if (valid_symbols[DECORATOR] && valid_symbols[DECORATOR_INLINE] && lexer->lookahead == '@') {
    advance(lexer);
    if (lexer->lookahead == '@') {
      advance(lexer);
    }

    if (is_decorator_start(lexer->lookahead)) {
      advance(lexer);

      if (lexer->lookahead == '"') {
        advance(lexer);
        while (lexer->lookahead != '"') {
          advance(lexer);
          if (lexer->eof(lexer)) {
            return false;
          }
        }
        advance(lexer);
        if (is_whitespace(lexer->lookahead)) {
          lexer->result_symbol = DECORATOR_INLINE;
          lexer->mark_end(lexer);
          return true;
        }
        if (lexer -> lookahead == '(') {
          lexer->result_symbol = DECORATOR;
          lexer->mark_end(lexer);
          return true;
        }
        return false;
      }

      while (is_decorator_identifier(lexer->lookahead)) {
        advance(lexer);
        if (lexer->eof(lexer)) {
          return false;
        }
      }

      if (is_whitespace(lexer->lookahead)) {
        lexer->result_symbol = DECORATOR_INLINE;
        lexer->mark_end(lexer);
        return true;
      }

      if (lexer->lookahead == '(') {
          lexer->result_symbol = DECORATOR;
          lexer->mark_end(lexer);
          return true;
      }
    }
    return false;
  }

  lexer->advance(lexer, iswspace(lexer->lookahead));
  return false;
}

// vim:sw=2
