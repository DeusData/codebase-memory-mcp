#include "tree_sitter/parser.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <wctype.h>

enum {
    NEWLINE,
    BACKSLASH,
    NL_COMMA,
    FLOAT,
    BLOCK_COMMENT,
    BRACKET,
    QUOTE,
};

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

void *tree_sitter_odin_external_scanner_create() { return NULL; }

unsigned tree_sitter_odin_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_odin_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {}

bool tree_sitter_odin_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[FLOAT]) {
        while (iswspace(lexer->lookahead) && lexer->lookahead != '\n') {
            skip(lexer);
        }

        if (!valid_symbols[NEWLINE]) { // skip newlines too
            while (iswspace(lexer->lookahead)) {
                skip(lexer);
            }
        }

        // basically, -? [0-9]+ \. [0-9]*, BUT a second . after isnt allowed
        // cuz it could be ..< operator,
        // it can have an i at the end for imaginary numbers
        // and exponents, [eE][+-]?[0-9]+, imaginary comes after

        // needs ONE of these two to be float
        bool found_decimal = false;
        bool found_exponent = false;
        bool found_number_before_decimal = false;
        bool found_number_after_decimal = false;
        bool found_number_after_expontent = false;
        for (int i = 0;; i++) {
            switch (lexer->lookahead) {
                case '.':
                    if ((found_decimal || found_exponent) &&
                        (found_number_after_decimal || found_number_before_decimal)) {
                        lexer->result_symbol = FLOAT;
                        lexer->mark_end(lexer);
                        return true;
                    } else {
                        lexer->mark_end(lexer);
                        found_decimal = true;
                        advance(lexer);
                        if (lexer->lookahead == '.') {
                            advance(lexer);
                            goto newline;
                        }
                        lexer->mark_end(lexer);
                        if (!isdigit(lexer->lookahead) && (found_number_after_decimal || found_number_before_decimal)) {
                            lexer->result_symbol = FLOAT;
                            return true;
                        }
                    }
                    break;
                case 'i':
                case 'j':
                case 'k':
                    if (!found_number_after_decimal) {
                        goto newline;
                    }
                    if ((found_decimal || found_exponent) &&
                        (found_number_after_decimal || found_number_before_decimal)) {
                        advance(lexer);
                        lexer->result_symbol = FLOAT;
                        lexer->mark_end(lexer);
                        return true;
                    }
                    goto newline;
                case 'e':
                case 'E':
                    if ((found_exponent) && (found_number_after_decimal || found_number_before_decimal)) {
                        lexer->result_symbol = FLOAT;
                        lexer->mark_end(lexer);
                        return true;
                    } else if (found_number_before_decimal || found_number_after_decimal) {
                        found_exponent = true;
                        advance(lexer);
                    } else {
                        goto newline;
                    }
                    break;
                case '+':
                case '-':
                    if (i == 0 || (found_exponent && !found_number_after_expontent)) {
                        advance(lexer);
                    } else {
                        goto newline;
                    }
                    break;
                default:
                    if (lexer->lookahead <= 255 && isdigit(lexer->lookahead)) {
                        advance(lexer);
                        if (found_decimal) {
                            found_number_after_decimal = true;
                        } else {
                            found_number_before_decimal = true;
                        }
                        if (found_exponent && !found_number_after_expontent) {
                            found_number_after_expontent = true;
                        }
                    } else {
                        if ((found_decimal || found_exponent) &&
                            (found_number_after_decimal || found_number_before_decimal)) {
                            lexer->result_symbol = FLOAT;
                            lexer->mark_end(lexer);
                            return true;
                        }
                        if (found_number_before_decimal) {
                            return false; // number needs to match
                        }
                        goto newline;
                    }
            }
        }
    }

    if (valid_symbols[NL_COMMA]) {
        while (iswspace(lexer->lookahead) && lexer->lookahead != '\n') {
            skip(lexer);
        }

        if (lexer->lookahead == ',') {
            advance(lexer);
            lexer->result_symbol = NL_COMMA;
            lexer->mark_end(lexer);
            while (iswspace(lexer->lookahead) && lexer->lookahead != '\n') {
                advance(lexer);
            }

            if (lexer->lookahead == '\n') {
                while (iswspace(lexer->lookahead)) {
                    advance(lexer);
                }
                return lexer->lookahead != '}';
            }
        }
    }

newline:
    if (valid_symbols[NEWLINE]) {
        while (iswspace(lexer->lookahead) && lexer->lookahead != '\n') {
            skip(lexer);
        }

        if (lexer->lookahead == '\n') {
            advance(lexer);
            lexer->result_symbol = NEWLINE;
            lexer->mark_end(lexer);

            uint32_t nl_count = 0;

            while (iswspace(lexer->lookahead)) {
                if (lexer->lookahead == '\n') {
                    nl_count++;
                }
                skip(lexer);
            }

            const char *where = "where";
            const char *_else = "else";
            const char *bracket = "{";

            char next_word[6] = {0};

            // check for where and _else

            for (int i = 0; i < 5; i++) {
                if (iswspace(lexer->lookahead)) {
                    break;
                }
                next_word[i] = (char)lexer->lookahead;
                advance(lexer);
            }

            if (strcmp(next_word, where) == 0 || strcmp(next_word, _else) == 0) {
                if (!iswspace(lexer->lookahead)) {
                    return true;
                }
                goto backslash;
            }

            if (strcmp(next_word, bracket) == 0 && nl_count == 0 && valid_symbols[BRACKET]) {
                return false;
            }

            return true;
        }
        // if (lexer->lookahead == ';') {
        //     advance(lexer);
        //     lexer->result_symbol = SEPARATOR;
        //     lexer->mark_end(lexer);
        //     while (iswspace(lexer->lookahead)) {
        //         advance(lexer);
        //     }
        //     return true;
        // }
    }

backslash:
    if (valid_symbols[BACKSLASH] && lexer->lookahead == '\\') {
        advance(lexer);
        if (lexer->lookahead == '\n') {
            advance(lexer);
            while (iswspace(lexer->lookahead)) {
                advance(lexer);
            }
            lexer->result_symbol = BACKSLASH;
            return true;
        }
    }

    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }

    if (valid_symbols[BLOCK_COMMENT] && lexer->lookahead == '/') {
        advance(lexer);
        if (lexer->lookahead != '*') {
            return false;
        }
        advance(lexer);

        if (lexer->lookahead == '"') {
            return false;
        }

        bool after_star = false;
        unsigned nesting_depth = 1;
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
                            lexer->result_symbol = BLOCK_COMMENT;
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

void tree_sitter_odin_external_scanner_destroy(void *payload) {}
