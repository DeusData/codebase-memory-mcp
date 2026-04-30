#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    L,
    CLASS_IDENTIFIER,
};

void *tree_sitter_smali_external_scanner_create() { return NULL; }

void tree_sitter_smali_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_smali_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_smali_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

static void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

bool tree_sitter_smali_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[L]) {
        while (iswspace(lexer->lookahead)) {
            skip(lexer);
        }

        if (lexer->lookahead == 'L') {
            lexer->result_symbol = L;
            advance(lexer);
            return true;
        }
    }

    if (valid_symbols[CLASS_IDENTIFIER]) {
        // any alnum, stop at /
        lexer->result_symbol = CLASS_IDENTIFIER;
        while (iswalnum(lexer->lookahead) || lexer->lookahead == '_' || lexer->lookahead == '-' ||
               lexer->lookahead == '$') {
            advance(lexer);
        }
        return true;
    }

    return false;
}
