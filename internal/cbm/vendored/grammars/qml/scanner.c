#include "tree_sitter/parser.h"
#include "typescript-scanner.h"

void *tree_sitter_qmljs_external_scanner_create() { return NULL; }
void tree_sitter_qmljs_external_scanner_destroy(void *p) {}
unsigned tree_sitter_qmljs_external_scanner_serialize(void *p, char *buffer) { return 0; }
void tree_sitter_qmljs_external_scanner_deserialize(void *p, const char *b, unsigned n) {}

bool tree_sitter_qmljs_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  return external_scanner_scan(payload, lexer, valid_symbols);
}
