#include "tree_sitter/alloc.h"
#include "tree_sitter/parser.h"

#include <wctype.h>

typedef enum {
    EXTERNAL_ASTERISK,
    MULTILINE_STRING_CONTENT,
} TokenType;

typedef struct {
    uint8_t quote_before_end_count;
} Scanner;

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

void *tree_sitter_bicep_external_scanner_create() { return ts_calloc(1, sizeof(Scanner)); }

void tree_sitter_bicep_external_scanner_destroy(void *payload) { ts_free(payload); }

unsigned tree_sitter_bicep_external_scanner_serialize(void *payload, char *buffer) {
    Scanner *scanner = (Scanner *)payload;
    buffer[0] = (char)scanner->quote_before_end_count;
    return 1;
}

void tree_sitter_bicep_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    if (length == 1) {
        Scanner *scanner = (Scanner *)payload;
        scanner->quote_before_end_count = buffer[0];
    }
}

bool tree_sitter_bicep_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;

    if (valid_symbols[EXTERNAL_ASTERISK]) {
        while (iswspace(lexer->lookahead)) {
            skip(lexer);
        }
        if (lexer->lookahead == '*') {
            advance(lexer);
            lexer->mark_end(lexer);
            lexer->result_symbol = EXTERNAL_ASTERISK;
            if (lexer->lookahead == ':') {
                return true;
            }
        }
    }

    if (valid_symbols[MULTILINE_STRING_CONTENT]) {
        bool advanced_once = false;
        while (!lexer->eof(lexer)) {
            if (lexer->lookahead == '\'') {
                if (scanner->quote_before_end_count > 0) {
                    while (scanner->quote_before_end_count > 0) {
                        advance(lexer);
                        scanner->quote_before_end_count--;
                    }

                    lexer->result_symbol = MULTILINE_STRING_CONTENT;
                    return true;
                }

                lexer->mark_end(lexer);
                advance(lexer);
                if (lexer->lookahead == '\'') {
                    advance(lexer);
                    if (lexer->lookahead == '\'') {
                        advance(lexer);

                        // How many quotes to advance on the next external scanner invocation
                        while (lexer->lookahead == '\'') {
                            scanner->quote_before_end_count++;
                            advance(lexer);
                        }

                        lexer->result_symbol = MULTILINE_STRING_CONTENT;
                        return advanced_once;
                    }
                }
            }
            advance(lexer);
            advanced_once = true;
        }
    }

    return false;
}
