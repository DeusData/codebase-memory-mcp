#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    HELP_TEXT,
};

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

void *tree_sitter_kconfig_external_scanner_create() { return NULL; }

void tree_sitter_kconfig_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_kconfig_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_kconfig_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

bool tree_sitter_kconfig_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[HELP_TEXT]) {
        uint32_t start_col = 0;
        while (iswspace(lexer->lookahead)) {
            switch (lexer->lookahead) {
                case ' ':
                    start_col++;
                    break;
                case '\t':
                    start_col += 8;
                    // Align col to next tab stop, ignore up to 7 leading spaces
                    start_col -= start_col % 8;
                    break;
                default:
                    break;
            }
            skip(lexer);
        }

    scan_line:
        while (lexer->lookahead != '\n' && !lexer->eof(lexer)) {
            advance(lexer);
        }

        if (lexer->eof(lexer)) {
            lexer->mark_end(lexer);
            lexer->result_symbol = HELP_TEXT;
            return true;
        }

        lexer->mark_end(lexer);
        uint32_t next_col = 0;
        while (iswspace(lexer->lookahead)) {
            switch (lexer->lookahead) {
                case ' ':
                    next_col++;
                    break;
                case '\t':
                    next_col += 8;
                    // Align col to next tab stop, ignore up to 7 leading spaces
                    next_col -= next_col % 8;
                    break;
                default:
                    break;
            }
            advance(lexer);
        }

        if (next_col >= start_col) {
            goto scan_line;
        }

        lexer->mark_end(lexer);
        lexer->result_symbol = HELP_TEXT;
        return true;
    }

    return false;
}
