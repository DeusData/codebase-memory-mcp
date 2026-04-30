#include "tree_sitter/parser.h"

#ifdef _MSC_VER
#pragma warning(disable : 4100)
#elif defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

enum TokenType { FAKE_EOL };

static bool reached_eof = false;

bool tree_sitter_properties_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  lexer->result_symbol = FAKE_EOL;
  return reached_eof = !reached_eof && valid_symbols[FAKE_EOL] && lexer->eof(lexer);
}

unsigned tree_sitter_properties_external_scanner_serialize(void *payload, char *buffer) {
  return reached_eof;
}

void tree_sitter_properties_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
  reached_eof = length;
}

void *tree_sitter_properties_external_scanner_create() { return NULL; }

void tree_sitter_properties_external_scanner_destroy(void *payload) { }
