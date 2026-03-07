#include "tree_sitter/parser.h"
#include <wctype.h>

enum TokenType {
  COMMENT,
};

void *tree_sitter_wolfram_external_scanner_create() { return NULL; }
void tree_sitter_wolfram_external_scanner_destroy(void *p) {}
void tree_sitter_wolfram_external_scanner_reset(void *p) {}
unsigned tree_sitter_wolfram_external_scanner_serialize(void *p, char *buffer) { return 0; }
void tree_sitter_wolfram_external_scanner_deserialize(void *p, const char *b, unsigned n) {}

static void advance(TSLexer *lexer) {
  lexer->advance(lexer, false);
}

bool tree_sitter_wolfram_external_scanner_scan(void *payload, TSLexer *lexer,
                                            const bool *valid_symbols) {
  while (iswspace(lexer->lookahead)) lexer->advance(lexer, true);

  if (lexer->lookahead == '(') {
    advance(lexer);
    if (lexer->lookahead != '*') return false;
    advance(lexer);

    bool after_star = false;
    unsigned nesting_depth = 1;
    for (;;) {
      switch (lexer->lookahead) {
        case '\0':
          return false;
        case '*':
          advance(lexer);
          after_star = true;
          break;
        case '(':
          advance(lexer);
          after_star = false;
          if (lexer->lookahead == '*') {
            nesting_depth++;
            advance(lexer);
          }
          break;
        case ')':
          if (after_star) {
            advance(lexer);
            after_star = false;
            nesting_depth--;
            if (nesting_depth == 0) {
              lexer->result_symbol = COMMENT;
              return true;
            }
          } else {
            advance(lexer);
            after_star = false;
          }
          break;
        default:
          advance(lexer);
          after_star = false;
          break;
      }
    }
  }

  return false;
}
