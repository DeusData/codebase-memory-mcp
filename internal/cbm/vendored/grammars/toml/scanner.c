#include "tree_sitter/parser.h"

typedef enum {
    LINE_ENDING_OR_EOF,
    MULTILINE_BASIC_STRING_CONTENT,
    MULTILINE_BASIC_STRING_END,
    MULTILINE_LITERAL_STRING_CONTENT,
    MULTILINE_LITERAL_STRING_END,
} TokenType;

void *tree_sitter_toml_external_scanner_create() { return NULL; }

void tree_sitter_toml_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_toml_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_toml_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

bool tree_sitter_toml_external_scanner_scan_multiline_string_end(TSLexer *lexer, const bool *valid_symbols,
                                                                 int32_t delimiter, TokenType content_symbol,
                                                                 TokenType end_symbol) {
    if (!valid_symbols[end_symbol] || lexer->lookahead != delimiter) {
        return false;
    }

    lexer->advance(lexer, false);
    lexer->mark_end(lexer);

    if (lexer->lookahead != delimiter) {
        lexer->result_symbol = content_symbol;
        return true;
    }

    lexer->advance(lexer, false);

    if (lexer->lookahead != delimiter) {
        lexer->mark_end(lexer);
        lexer->result_symbol = content_symbol;
        return true;
    }

    lexer->advance(lexer, false);

    if (lexer->lookahead != delimiter) {
        lexer->mark_end(lexer);
        lexer->result_symbol = end_symbol;
        return true;
    }

    lexer->result_symbol = content_symbol;
    return true;
}

bool tree_sitter_toml_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    if (tree_sitter_toml_external_scanner_scan_multiline_string_end(
            lexer, valid_symbols, '"', MULTILINE_BASIC_STRING_CONTENT, MULTILINE_BASIC_STRING_END) ||
        tree_sitter_toml_external_scanner_scan_multiline_string_end(
            lexer, valid_symbols, '\'', MULTILINE_LITERAL_STRING_CONTENT, MULTILINE_LITERAL_STRING_END)) {
        return true;
    }

    if (valid_symbols[LINE_ENDING_OR_EOF]) {
        lexer->result_symbol = LINE_ENDING_OR_EOF;

        while (lexer->lookahead == ' ' || lexer->lookahead == '\t') {
            lexer->advance(lexer, true);
        }

        if (lexer->lookahead == 0 || lexer->lookahead == '\n') {
            return true;
        }

        if (lexer->lookahead == '\r') {
            lexer->advance(lexer, true);
            if (lexer->lookahead == '\n') {
                return true;
            }
        }
    }

    return false;
}