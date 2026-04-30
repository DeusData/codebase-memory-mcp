#include "tree_sitter/parser.h"
#include <wctype.h>

enum TokenType {
  CONCAT,
  IMMEDIATE
};

void *tree_sitter_tcl_external_scanner_create() {
  return NULL;
}

bool tree_sitter_tcl_external_scanner_scan(void *payload, TSLexer *lexer,
                                          const bool *valid_symbols) {
  int32_t c = lexer->lookahead;

  if (valid_symbols[IMMEDIATE] && !iswspace(c)) {
    lexer->result_symbol = IMMEDIATE;
    return false;
  }

  if (valid_symbols[CONCAT] && (
        !iswspace(c) &&
        c != ')' &&
        c != ':' &&
        c != '}' &&
        c != ']')) {
    lexer->result_symbol = CONCAT;
    return true;
  }

  return false;
}

unsigned tree_sitter_tcl_external_scanner_serialize(void *payload, char *state) {
  return 0;
}

void tree_sitter_tcl_external_scanner_deserialize(void *payload, const char *state, unsigned length){ }

void tree_sitter_tcl_external_scanner_destroy(void *payload) {}
