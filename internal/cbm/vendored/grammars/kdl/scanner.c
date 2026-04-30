#include "tree_sitter/parser.h"
#include "wctype.h"
#include <ctype.h>

enum { _EOF, MULTI_LINE_COMMENT, _RAW_STRING };

void *tree_sitter_kdl_external_scanner_create() { return NULL; }

void tree_sitter_kdl_external_scanner_destroy(void *payload) {}

unsigned tree_sitter_kdl_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_kdl_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

static void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

bool tree_sitter_kdl_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    // check for End-of-file
    if (valid_symbols[_EOF] && lexer->lookahead == 0) {
        lexer->result_symbol = _EOF;
        advance(lexer);
        return true;
    }

    if (valid_symbols[_RAW_STRING] && lexer->lookahead == 'r') {
        advance(lexer);

        unsigned num_hashes = 0;
        while (lexer->lookahead == '#') {
            num_hashes += 1;
            advance(lexer);
        }

        if (lexer->lookahead != '"') {
            return false;
        }

        advance(lexer);

        for (;;) {
            if (lexer->eof(lexer)) {
                // Unclosed raw string caused by EOF.
                return false;
            }

            int32_t c = lexer->lookahead;
            advance(lexer);

            if (c != '"') {
                continue;
            }

            // Try to match `num_hashes` closing hashes.
            unsigned closing_hashes = 0;
            for (;;) {
                if (closing_hashes == num_hashes) {
                    goto success;
                }

                if (lexer->lookahead != '#') {
                    break;
                }

                advance(lexer);

                closing_hashes += 1;
            }
        }

    success:
        lexer->result_symbol = _RAW_STRING;
        return true;
    }

    // multi-line-comment := '/*' commented-block
    if (lexer->lookahead == '/') {
        advance(lexer);
        if (lexer->lookahead != '*')
            return false;
        advance(lexer);

        bool after_star = false;
        unsigned nesting_depth = 1;
        // commented-block := '*/' | (multi-line-comment | '*' | '/' | [^*/]+)
        // commented-block
        for (;;) {
            switch (lexer->lookahead) {
                case '\0':
                    return false;
                case '*':
                    advance(lexer);
                    after_star = true;
                    break;
                case '/':
                    if (after_star) {
                        advance(lexer);
                        after_star = false;
                        nesting_depth--;
                        if (nesting_depth == 0) {
                            lexer->result_symbol = MULTI_LINE_COMMENT;
                            return true;
                        }
                    } else {
                        advance(lexer);
                        after_star = false;
                        if (lexer->lookahead == '*') {
                            nesting_depth++;
                            advance(lexer);
                        }
                    }
                    break;
                default:
                    advance(lexer);
                    after_star = false;
                    break;
            }
        }
    }

    return false;
}
