#include "tree_sitter/parser.h"
#include <wctype.h>

// Copied and adjusted from
// https://github.com/tree-sitter/tree-sitter-rust/blob/master/src/scanner.c
// under MIT

enum TokenType {
    MULTILINE_COMMENT,
};

void *tree_sitter_tablegen_external_scanner_create() { return NULL; }

void tree_sitter_tablegen_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_tablegen_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_tablegen_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

bool tree_sitter_tablegen_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[MULTILINE_COMMENT]) {
        while (iswspace(lexer->lookahead)) {
            lexer->advance(lexer, true);
        }

        if (lexer->lookahead != '/') {
            return false;
        }
        lexer->advance(lexer, false);
        if (lexer->lookahead != '*') {
            return false;
        }
        lexer->advance(lexer, false);

        bool after_star = false;
        unsigned nesting_depth = 1;
        for (;;) {
            switch (lexer->lookahead) {
                case '\0':
                    return false;
                case '*':
                    lexer->advance(lexer, false);
                    after_star = true;
                    break;
                case '/':
                    if (after_star) {
                        lexer->advance(lexer, false);
                        after_star = false;
                        nesting_depth--;
                        if (nesting_depth == 0) {
                            lexer->result_symbol = MULTILINE_COMMENT;
                            return true;
                        }
                    } else {
                        lexer->advance(lexer, false);
                        after_star = false;
                        if (lexer->lookahead == '*') {
                            nesting_depth++;
                            lexer->advance(lexer, false);
                        }
                    }
                    break;
                default:
                    lexer->advance(lexer, false);
                    after_star = false;
                    break;
            }
        }
    }

    return false;
}
