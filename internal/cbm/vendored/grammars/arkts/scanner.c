// ArkTS external scanner — verbatim tree-sitter-typescript scanner logic.
//
// The scanner body lives in _common_scanner.h, a byte-identical copy of
// tree-sitter/tree-sitter-typescript common/scanner.h @ 75b3874edb2d
// (v0.23.2). Only the exported symbol names differ (arkts instead of
// typescript) because tree-sitter derives them from the grammar name.
// The external token set and order are identical to the typescript
// dialect's, which is what makes the verbatim reuse correct.
#include "_common_scanner.h"

void *tree_sitter_arkts_external_scanner_create() { return NULL; }

void tree_sitter_arkts_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_arkts_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_arkts_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

bool tree_sitter_arkts_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    return external_scanner_scan(payload, lexer, valid_symbols);
}
