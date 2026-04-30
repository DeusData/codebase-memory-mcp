#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    VERBATIM_STRING,
};

void *tree_sitter_squirrel_external_scanner_create() { return NULL; }

void tree_sitter_squirrel_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_squirrel_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_squirrel_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

static void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

bool tree_sitter_squirrel_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    while (iswspace(lexer->lookahead)) {
        lexer->advance(lexer, true);
    }

    if (valid_symbols[VERBATIM_STRING] && lexer->lookahead == '@') {
        lexer->result_symbol = VERBATIM_STRING;
        advance(lexer);

        if (lexer->lookahead != '"') {
            return false;
        }
        advance(lexer);

        for (;;) {
            if (lexer->lookahead == 0) {
                return false;
            }
            if (lexer->lookahead == '"') {
                advance(lexer);
                // so in Squirrel two quotes is a normal quote
                if (lexer->lookahead == '"') {
                    advance(lexer);
                } else {
                    lexer->mark_end(lexer);
                    return true;
                }
            } else {
                advance(lexer);
            }
        }
    }

    return false;
}
