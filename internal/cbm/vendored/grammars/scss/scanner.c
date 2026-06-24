#include "tree_sitter/parser.h"
#include <wctype.h>

enum TokenType {
  DESCENDANT_OP,
};

void *tree_sitter_scss_external_scanner_create() { return NULL; }
void tree_sitter_scss_external_scanner_destroy(void *p) {}
void tree_sitter_scss_external_scanner_reset(void *p) {}
unsigned tree_sitter_scss_external_scanner_serialize(void *p, char *buffer) { return 0; }
void tree_sitter_scss_external_scanner_deserialize(void *p, const char *b, unsigned n) {}

bool tree_sitter_scss_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  if (iswspace(lexer->lookahead) && valid_symbols[DESCENDANT_OP]) {
    lexer->result_symbol = DESCENDANT_OP;

    lexer->advance(lexer, true);
    while (iswspace(lexer->lookahead)) {
      lexer->advance(lexer, true);
    }
    lexer->mark_end(lexer);

    if (
      lexer->lookahead == '#' ||
      lexer->lookahead == '.' ||
      lexer->lookahead == '[' ||
      lexer->lookahead == '-' ||
      lexer->lookahead == '&' ||
      iswalnum(lexer->lookahead)
    ) {
      return true;
    }

    if (lexer->lookahead == ':') {
      lexer->advance(lexer, false);
      if (iswspace(lexer->lookahead)) return false;
      for (;;) {
        if (
          lexer->lookahead == ';' ||
          lexer->lookahead == '}' ||
          lexer->eof(lexer)
        ) return false;
        if (lexer->lookahead == '{') {
          return true;
        }
        lexer->advance(lexer, false);
      }
    }
  }

  return false;
}