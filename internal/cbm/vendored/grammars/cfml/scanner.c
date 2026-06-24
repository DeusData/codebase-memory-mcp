#include "../../common/scanner.h"

void *tree_sitter_cfml_external_scanner_create() {
    Scanner *scanner = (Scanner *)ts_calloc(1, sizeof(Scanner));
    return scanner;
}

bool tree_sitter_cfml_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    return external_scanner_scan(payload, lexer, valid_symbols, SCANNER_SYMBOL_COUNT, false);
}

unsigned tree_sitter_cfml_external_scanner_serialize(void *payload, char *buffer) {
    Scanner *scanner = (Scanner *)payload;
    return serialize(scanner, buffer, false);
}

void tree_sitter_cfml_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    Scanner *scanner = (Scanner *)payload;
    deserialize(scanner, buffer, length, false);
}

void tree_sitter_cfml_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    for (unsigned i = 0; i < scanner->tags.size; i++) {
        tag_free(&scanner->tags.contents[i]);
    }
    array_delete(&scanner->tags);

    for (unsigned i = 0; i < scanner->cf_tags.size; i++) {
        tag_free(&scanner->cf_tags.contents[i]);
    }
    array_delete(&scanner->cf_tags);
    ts_free(scanner);
}
