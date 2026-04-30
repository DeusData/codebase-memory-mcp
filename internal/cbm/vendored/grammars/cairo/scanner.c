#include "tree_sitter/parser.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wctype.h>

enum TokenType {
    HINT_START,
    PYTHON_CODE_LINE,
    FAILURE,
};

enum Context {
    C_NONE,
    C_PYTHON_CODE,
    C_PYTHON_STRING,
    C_PYTHON_COMMENT,
};

enum PythonStringType {
    PST_NONE,
    PST_1_SQ_STRING,
    PST_3_SQ_STRING,
    PST_1_DQ_STRING,
    PST_3_DQ_STRING,
};

typedef struct {
    uint32_t ws_count;
    uint8_t context;
    uint8_t pst;
} Scanner;

static bool scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[FAILURE]) {
        return false;
    }

    // In Cairo, hints start with %{ and end with %} and can contain anything
    // including %s in between and start / end tokens inside of Python strings
    if (valid_symbols[HINT_START]) {
        if (lexer->lookahead == '%') {
            lexer->advance(lexer, true);
            if (lexer->lookahead == '{') {
                scanner->context = C_PYTHON_CODE;
                // Fallback to a built-in lexer
                return false;
            }
        }
    }

    if ((valid_symbols[PYTHON_CODE_LINE])) {
        // Skip the first \n after `%{` token,
        // all trailing \n after code lines will be included to themselves
        if (lexer->lookahead == '\n') {
            lexer->advance(lexer, true);
        }

        // There is a standalone hint close on line, don't consume it,
        // it's a job of a built-in lexer
        if (lexer->lookahead == '%') {
            lexer->mark_end(lexer);
            lexer->advance(lexer, false);
            if (lexer->lookahead == '}') {
                if (scanner->context == C_PYTHON_STRING) {
                    lexer->result_symbol = FAILURE;
                    return true;
                }

                scanner->context = C_NONE;
                return false;
            }
        }

        // Skip whitespaces before the hint content
        // and count them to be able to restore the position
        // after every line
        uint32_t ws_count = 0;
        while (!lexer->eof(lexer)) {
            if (lexer->lookahead == '\n') {
                lexer->advance(lexer, false);
                lexer->mark_end(lexer);
                lexer->result_symbol = PYTHON_CODE_LINE;
                return true;
            }
            if (iswspace(lexer->lookahead)) {
                ws_count += lexer->lookahead == '\t' ? 8 : 1;
                lexer->advance(lexer, true);
                if (scanner->ws_count > 0 && ws_count == scanner->ws_count) {
                    break;
                }
            } else {
                // Make parsing redundant to improperly formated python code.
                if (scanner->ws_count == 0 || ws_count < scanner->ws_count) {
                    scanner->ws_count = ws_count;
                }
                break;
            }
        }

        uint32_t content_len = 0;
        while (!lexer->eof(lexer)) {
            switch (lexer->lookahead) {
                case '\'':
                case '"': {
                    const char chr = (char)lexer->lookahead;
                    lexer->advance(lexer, false);
                    content_len++;
                    if (scanner->context == C_PYTHON_STRING) {
                        unsigned iter = scanner->pst == PST_1_DQ_STRING ||
                                                scanner->pst == PST_1_SQ_STRING
                                            ? 0
                                            : 2;
                        if (iter > 0) {
                            do {
                                if (lexer->lookahead != chr) {
                                    scanner->context = C_PYTHON_CODE;
                                    scanner->pst = PST_NONE;
                                    return false;
                                }
                                lexer->advance(lexer, false);
                                content_len++;
                            } while (--iter);
                        }
                        scanner->context = C_PYTHON_CODE;
                        scanner->pst = PST_NONE;
                        continue;
                    }
                    if (lexer->lookahead == chr) {
                        lexer->advance(lexer, false);
                        content_len++;
                        if (lexer->lookahead == chr) {
                            lexer->advance(lexer, false);
                            content_len++;
                            scanner->context = C_PYTHON_STRING;
                            scanner->pst =
                                chr == '"' ? PST_3_DQ_STRING : PST_3_SQ_STRING;
                        } else {
                            // single/double string ended, '' or ""
                            scanner->context = C_PYTHON_CODE;
                            scanner->pst = PST_NONE;
                        }
                    } else {
                        scanner->context = C_PYTHON_STRING;
                        scanner->pst =
                            chr == '"' ? PST_1_DQ_STRING : PST_1_SQ_STRING;
                    }

                    continue;
                }
                case '%':
                    if (scanner->context == C_PYTHON_STRING) {
                        lexer->advance(lexer, false);
                        content_len++;
                        continue;
                    }

                    lexer->mark_end(lexer);
                    lexer->advance(lexer, false);
                    if (lexer->lookahead == '}') {
                        if (scanner->context == C_PYTHON_STRING) {
                            lexer->result_symbol = FAILURE;
                            return true;
                        }

                        scanner->context = C_NONE;
                        // Don't produce an empty node before a hint close token
                        if (content_len > 0) {
                            lexer->result_symbol = PYTHON_CODE_LINE;
                            return true;
                        }
                        return false;
                    }
                    break;

                case '\n':
                    lexer->advance(lexer, false);
                    lexer->mark_end(lexer);
                    lexer->result_symbol = PYTHON_CODE_LINE;
                    return true;

                case '#':
                    if (scanner->context == C_PYTHON_STRING) {
                        lexer->advance(lexer, false);
                        content_len++;
                        continue;
                    } else {
                        scanner->context = C_PYTHON_COMMENT;
                        while (lexer->lookahead != '\n' && !lexer->eof(lexer)) {
                            lexer->advance(lexer, false);
                            content_len++;
                        }
                        scanner->context = C_NONE;
                        continue;
                    }

                default:
                    lexer->advance(lexer, false);
                    content_len++;
            }
        }
    }

    return false;
}

void *tree_sitter_cairo_external_scanner_create() {
    Scanner *scanner = (Scanner *)calloc(1, sizeof(Scanner));
    assert(scanner != NULL && "Failed to allocate memory for scanner");
    return scanner;
}

bool tree_sitter_cairo_external_scanner_scan(void *payload, TSLexer *lexer,
                                             const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;
    return scan(scanner, lexer, valid_symbols);
}

unsigned tree_sitter_cairo_external_scanner_serialize(void *payload,
                                                      char *buffer) {
    unsigned len = sizeof(Scanner);
    memcpy(buffer, payload, len);
    return len;
}

void tree_sitter_cairo_external_scanner_deserialize(void *payload,
                                                    const char *buffer,
                                                    unsigned length) {
    Scanner *scanner = (Scanner *)payload;
    if (length > 0) {
        assert(sizeof(Scanner) == length && "sizeof(Scanner) != length");
        memcpy(scanner, buffer, sizeof(Scanner));
    }
}

void tree_sitter_cairo_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    free(scanner);
}
