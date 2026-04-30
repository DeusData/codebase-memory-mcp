#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    BLOCK_COMMENT_START,
    BLOCK_COMMENT_CONTENT,
    BLOCK_COMMENT_END,

    STRING_START,
    STRING_CONTENT,
    STRING_END,
};

typedef struct {
    char ending_char;
    uint8_t level_count;
} Scanner;

static inline void reset_state(Scanner *scanner) {
    scanner->ending_char = 0;
    scanner->level_count = 0;
}

static inline void consume(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static inline bool consume_char(char chr, TSLexer *lexer) {
    if (lexer->lookahead != chr) {
        return false;
    }

    consume(lexer);
    return true;
}

static inline uint8_t consume_and_count_char(char chr, TSLexer *lexer) {
    uint8_t count = 0;
    while (lexer->lookahead == chr) {
        ++count;
        consume(lexer);
    }
    return count;
}

static inline void skip_whitespaces(TSLexer *lexer) {
    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }
}

void *tree_sitter_luau_external_scanner_create() {
    Scanner *scanner = calloc(1, sizeof(Scanner));
    return scanner;
}

void tree_sitter_luau_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    free(scanner);
}

unsigned tree_sitter_luau_external_scanner_serialize(void *payload, char *buffer) {
    Scanner *scanner = (Scanner *)payload;
    buffer[0] = scanner->ending_char;
    buffer[1] = (char)scanner->level_count;
    return 2;
}

void tree_sitter_luau_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    Scanner *scanner = (Scanner *)payload;
    if (length == 0) {
        return;
    }
    scanner->ending_char = buffer[0];
    if (length == 1) {
        return;
    }
    scanner->level_count = buffer[1];
}

static bool scan_block_start(Scanner *scanner, TSLexer *lexer) {
    if (consume_char('[', lexer)) {
        uint8_t level = consume_and_count_char('=', lexer);

        if (consume_char('[', lexer)) {
            scanner->level_count = level;
            return true;
        }
    }

    return false;
}

static bool scan_block_end(Scanner *scanner, TSLexer *lexer) {
    if (consume_char(']', lexer)) {
        uint8_t level = consume_and_count_char('=', lexer);

        if (scanner->level_count == level && consume_char(']', lexer)) {
            return true;
        }
    }

    return false;
}

static bool scan_block_content(Scanner *scanner, TSLexer *lexer) {
    while (lexer->lookahead != 0) {
        if (lexer->lookahead == ']') {
            lexer->mark_end(lexer);

            if (scan_block_end(scanner, lexer)) {
                return true;
            }
        } else {
            consume(lexer);
        }
    }

    return false;
}

static bool scan_comment_start(Scanner *scanner, TSLexer *lexer) {
    if (consume_char('-', lexer) && consume_char('-', lexer)) {
        lexer->mark_end(lexer);

        if (scan_block_start(scanner, lexer)) {
            lexer->mark_end(lexer);
            lexer->result_symbol = BLOCK_COMMENT_START;
            return true;
        }
    }

    return false;
}

static bool scan_comment_content(Scanner *scanner, TSLexer *lexer) {
    if (scanner->ending_char == 0) { // block comment
        if (scan_block_content(scanner, lexer)) {
            lexer->result_symbol = BLOCK_COMMENT_CONTENT;
            return true;
        }

        return false;
    }

    while (lexer->lookahead != 0) {
        if (lexer->lookahead == scanner->ending_char) {
            reset_state(scanner);
            lexer->result_symbol = BLOCK_COMMENT_CONTENT;
            return true;
        }

        consume(lexer);
    }

    return false;
}

static bool scan_string_start(Scanner *scanner, TSLexer *lexer) {
    if (lexer->lookahead == '"' || lexer->lookahead == '\'') {
        scanner->ending_char = (char)lexer->lookahead;
        consume(lexer);
        return true;
    }

    if (scan_block_start(scanner, lexer)) {
        return true;
    }

    return false;
}

static bool scan_string_end(Scanner *scanner, TSLexer *lexer) {
    if (scanner->ending_char == 0) { // block string
        return scan_block_end(scanner, lexer);
    }

    if (consume_char(scanner->ending_char, lexer)) {
        return true;
    }

    return false;
}

static bool scan_string_content(Scanner *scanner, TSLexer *lexer) {
    if (scanner->ending_char == 0) { // block string
        return scan_block_content(scanner, lexer);
    }

    while (lexer->lookahead != '\n' && lexer->lookahead != 0 && lexer->lookahead != scanner->ending_char) {
        if (consume_char('\\', lexer) && consume_char('z', lexer)) {
            while (iswspace(lexer->lookahead)) {
                consume(lexer);
            }
            continue;
        };

        if (lexer->lookahead == 0) {
            return true;
        }

        consume(lexer);
    }

    return true;
}

bool tree_sitter_luau_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;
    if (valid_symbols[STRING_END] && scan_string_end(scanner, lexer)) {
        reset_state(scanner);
        lexer->result_symbol = STRING_END;
        return true;
    }

    if (valid_symbols[STRING_CONTENT] && scan_string_content(scanner, lexer)) {
        lexer->result_symbol = STRING_CONTENT;
        return true;
    }

    if (valid_symbols[BLOCK_COMMENT_END] && scanner->ending_char == 0 && scan_block_end(scanner, lexer)) {
        reset_state(scanner);
        lexer->result_symbol = BLOCK_COMMENT_END;
        return true;
    }

    if (valid_symbols[BLOCK_COMMENT_CONTENT] && scan_comment_content(scanner, lexer)) {
        return true;
    }

    skip_whitespaces(lexer);

    if (valid_symbols[STRING_START] && scan_string_start(scanner, lexer)) {
        lexer->result_symbol = STRING_START;
        return true;
    }

    if (valid_symbols[BLOCK_COMMENT_START]) {
        if (scan_comment_start(scanner, lexer)) {
            return true;
        }
    }

    return false;
}
