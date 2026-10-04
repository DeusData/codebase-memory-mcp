#include "tree_sitter/alloc.h"
#include "tree_sitter/array.h"
#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    OPT_SEMI,
    INTERPOLATION_REGULAR_START,
    INTERPOLATION_VERBATIM_START,
    INTERPOLATION_RAW_START,
    INTERPOLATION_START_QUOTE,
    INTERPOLATION_END_QUOTE,
    INTERPOLATION_OPEN_BRACE,
    INTERPOLATION_CLOSE_BRACE,
    INTERPOLATION_STRING_CONTENT,
    RAW_STRING_START,
    RAW_STRING_END,
    RAW_STRING_CONTENT,
};

typedef enum {
    REGULAR = 1 << 0,
    VERBATIM = 1 << 1,
    RAW = 1 << 2,
} StringType;

typedef struct {
    uint8_t dollar_count;
    uint8_t open_brace_count;
    uint8_t quote_count;
    StringType string_type;
} Interpolation;

static inline bool is_regular(Interpolation *interpolation) { return interpolation->string_type & REGULAR; }

static inline bool is_verbatim(Interpolation *interpolation) { return interpolation->string_type & VERBATIM; }

static inline bool is_raw(Interpolation *interpolation) { return interpolation->string_type & RAW; }

typedef struct {
    uint8_t quote_count;
    Array(Interpolation) interpolation_stack;
} Scanner;

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

void *tree_sitter_c_sharp_external_scanner_create() {
    Scanner *scanner = ts_calloc(1, sizeof(Scanner));
    array_init(&scanner->interpolation_stack);
    return scanner;
}

void tree_sitter_c_sharp_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    array_delete(&scanner->interpolation_stack);
    ts_free(scanner);
}

unsigned tree_sitter_c_sharp_external_scanner_serialize(void *payload, char *buffer) {
    Scanner *scanner = (Scanner *)payload;

    if (scanner->interpolation_stack.size * 4 + 2 > TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
        return 0;
    }

    unsigned size = 0;

    buffer[size++] = (char)scanner->quote_count;
    buffer[size++] = (char)scanner->interpolation_stack.size;

    for (unsigned i = 0; i < scanner->interpolation_stack.size; i++) {
        Interpolation interpolation = scanner->interpolation_stack.contents[i];
        buffer[size++] = (char)interpolation.dollar_count;
        buffer[size++] = (char)interpolation.open_brace_count;
        buffer[size++] = (char)interpolation.quote_count;
        buffer[size++] = (char)interpolation.string_type;
    }

    return size;
}

void tree_sitter_c_sharp_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    Scanner *scanner = (Scanner *)payload;

    scanner->quote_count = 0;
    array_clear(&scanner->interpolation_stack);
    unsigned size = 0;

    if (length > 0) {
        scanner->quote_count = (unsigned char)buffer[size++];
        scanner->interpolation_stack.size = (unsigned char)buffer[size++];
        array_reserve(&scanner->interpolation_stack, scanner->interpolation_stack.size);

        for (unsigned i = 0; i < scanner->interpolation_stack.size; i++) {
            Interpolation interpolation = {0};
            interpolation.dollar_count = buffer[size++];
            interpolation.open_brace_count = buffer[size++];
            interpolation.quote_count = buffer[size++];
            interpolation.string_type = (unsigned char)buffer[size++];
            scanner->interpolation_stack.contents[i] = interpolation;
        }
    }

    assert(size == length);
}

bool tree_sitter_c_sharp_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;

    uint8_t brace_advanced = 0;
    uint8_t quote_count = 0;
    bool did_advance = false;

    // error recovery, gives better trees this way
    if (valid_symbols[OPT_SEMI] && valid_symbols[INTERPOLATION_REGULAR_START]) {
        return false;
    }

    if (valid_symbols[OPT_SEMI]) {
        lexer->result_symbol = OPT_SEMI;
        if (lexer->lookahead == ';') {
            advance(lexer);
        }
        return true;
    }

    if (valid_symbols[RAW_STRING_START]) {
        while (iswspace(lexer->lookahead)) {
            skip(lexer);
        }

        if (lexer->lookahead == '"') {
            while (lexer->lookahead == '"') {
                advance(lexer);
                quote_count++;
            }

            if (quote_count >= 3) {
                lexer->result_symbol = RAW_STRING_START;
                scanner->quote_count = quote_count;
                return true;
            }
        }
    }

    if (valid_symbols[RAW_STRING_END] && lexer->lookahead == '"') {
        while (lexer->lookahead == '"') {
            advance(lexer);
            quote_count++;
        }

        if (quote_count == scanner->quote_count) {
            lexer->result_symbol = RAW_STRING_END;
            scanner->quote_count = 0;
            return true;
        }

        did_advance = quote_count > 0;
    }

    if (valid_symbols[RAW_STRING_CONTENT]) {
        while (lexer->lookahead) {
            if (lexer->lookahead == '"') {
                lexer->mark_end(lexer);
                quote_count = 0;

                while (lexer->lookahead == '"') {
                    advance(lexer);
                    quote_count++;
                }

                if (quote_count == scanner->quote_count) {
                    lexer->result_symbol = RAW_STRING_CONTENT;
                    return true;
                }
            }
            advance(lexer);
            did_advance = true;
        }
        lexer->mark_end(lexer);
        lexer->result_symbol = RAW_STRING_CONTENT;
        return true;
    }

    if (valid_symbols[INTERPOLATION_REGULAR_START] || valid_symbols[INTERPOLATION_VERBATIM_START] ||
        valid_symbols[INTERPOLATION_RAW_START]) {
        while (iswspace(lexer->lookahead)) {
            skip(lexer);
        }

        uint8_t dollar_advanced = 0;

        bool is_verbatim = false;

        if (lexer->lookahead == '@') {
            is_verbatim = true;
            advance(lexer);
        }

        while (lexer->lookahead == '$' && quote_count == 0) {
            advance(lexer);
            dollar_advanced++;
        }

        if (dollar_advanced > 0 && (lexer->lookahead == '"' || lexer->lookahead == '@')) {
            lexer->result_symbol = INTERPOLATION_REGULAR_START;
            Interpolation interpolation = {
                .dollar_count = dollar_advanced,
                .open_brace_count = 0,
                .quote_count = 0,
                .string_type = 0,
            };

            if (is_verbatim || lexer->lookahead == '@') {
                if (lexer->lookahead == '@') {
                    advance(lexer);
                    is_verbatim = true;
                }
                lexer->result_symbol = INTERPOLATION_VERBATIM_START;
                interpolation.string_type = VERBATIM;
            }

            lexer->mark_end(lexer);
            advance(lexer);

            if (lexer->lookahead == '"' && !is_verbatim) {
                advance(lexer);
                if (lexer->lookahead == '"') {
                    lexer->result_symbol = INTERPOLATION_RAW_START;
                    interpolation.string_type |= RAW;
                    array_push(&scanner->interpolation_stack, interpolation);
                }
                // If we find 1 or 3 quotes, we push an interpolation.
                // If there's only two quotes, that's just an empty string
            } else {
                interpolation.string_type |= REGULAR;
                array_push(&scanner->interpolation_stack, interpolation);
            }

            return true;
        }
    }

    if (valid_symbols[INTERPOLATION_START_QUOTE] && scanner->interpolation_stack.size > 0) {
        Interpolation *current_interpolation = array_back(&scanner->interpolation_stack);

        if (is_verbatim(current_interpolation) || is_regular(current_interpolation)) {
            if (lexer->lookahead == '"') {
                advance(lexer);
                current_interpolation->quote_count++;
            }
        } else {
            while (lexer->lookahead == '"') {
                advance(lexer);
                current_interpolation->quote_count++;
            }
        }

        lexer->result_symbol = INTERPOLATION_START_QUOTE;
        return current_interpolation->quote_count > 0;
    }

    if (valid_symbols[INTERPOLATION_END_QUOTE] && scanner->interpolation_stack.size > 0) {
        Interpolation *current_interpolation = array_back(&scanner->interpolation_stack);

        while (lexer->lookahead == '"') {
            advance(lexer);
            quote_count++;
        }

        if (quote_count == current_interpolation->quote_count) {
            lexer->result_symbol = INTERPOLATION_END_QUOTE;
            array_pop(&scanner->interpolation_stack);
            return true;
        }

        did_advance = quote_count > 0;
    }

    if (valid_symbols[INTERPOLATION_OPEN_BRACE] && scanner->interpolation_stack.size > 0) {
        Interpolation *current_interpolation = array_back(&scanner->interpolation_stack);

        while (lexer->lookahead == '{' && brace_advanced < current_interpolation->dollar_count) {
            advance(lexer);
            brace_advanced++;
        }

        if (brace_advanced > 0 && brace_advanced == current_interpolation->dollar_count &&
            (brace_advanced == 0 || lexer->lookahead != '{')) {
            current_interpolation->open_brace_count = brace_advanced;
            lexer->result_symbol = INTERPOLATION_OPEN_BRACE;
            return true;
        }
    }

    if (valid_symbols[INTERPOLATION_CLOSE_BRACE] && scanner->interpolation_stack.size > 0) {
        uint8_t brace_advanced = 0;
        Interpolation *current_interpolation = array_back(&scanner->interpolation_stack);

        while (iswspace(lexer->lookahead)) {
            advance(lexer);
        }

        while (lexer->lookahead == '}') {
            advance(lexer);
            brace_advanced++;

            if (brace_advanced == current_interpolation->open_brace_count) {
                current_interpolation->open_brace_count = 0;
                lexer->result_symbol = INTERPOLATION_CLOSE_BRACE;
                return true;
            }
        }

        return false;
    }

    if (valid_symbols[INTERPOLATION_STRING_CONTENT] && scanner->interpolation_stack.size > 0) {
        lexer->result_symbol = INTERPOLATION_STRING_CONTENT;
        Interpolation *current_interpolation = array_back(&scanner->interpolation_stack);

        while (lexer->lookahead) {
            // top-down approach, first see if it's raw
            if (is_raw(current_interpolation)) {
                if (lexer->lookahead == '"') {
                    lexer->mark_end(lexer);
                    advance(lexer);
                    if (lexer->lookahead == '"') {
                        advance(lexer);
                        uint8_t quote_advanced = 2;
                        while (lexer->lookahead == '"') {
                            quote_advanced++;
                            advance(lexer);
                        }
                        if (quote_advanced == current_interpolation->quote_count) {
                            return did_advance;
                        }
                    }
                }

                if (lexer->lookahead == '{') {
                    lexer->mark_end(lexer);

                    while (lexer->lookahead == '{' && brace_advanced < current_interpolation->open_brace_count) {
                        advance(lexer);
                        brace_advanced++;
                    }

                    if (brace_advanced == current_interpolation->open_brace_count &&
                        (brace_advanced == 0 || lexer->lookahead != '{')) {
                        return did_advance;
                    }
                }
            }

            // then verbatim, since it could be verbatim + raw, but run the raw branch first
            else if (is_verbatim(current_interpolation)) {
                if (lexer->lookahead == '"') {
                    lexer->mark_end(lexer);
                    advance(lexer);
                    if (lexer->lookahead == '"') {
                        advance(lexer);
                        continue;
                    }
                    return did_advance;
                }

                if (lexer->lookahead == '{') {
                    lexer->mark_end(lexer);

                    while (lexer->lookahead == '{' && brace_advanced < current_interpolation->open_brace_count) {
                        advance(lexer);
                        brace_advanced++;
                    }

                    if (brace_advanced == current_interpolation->open_brace_count &&
                        (brace_advanced == 0 || lexer->lookahead != '{')) {
                        return did_advance;
                    }
                }
            }

            // finally regular
            else if (is_regular(current_interpolation)) {
                if (lexer->lookahead == '\\' || lexer->lookahead == '\n' || lexer->lookahead == '"') {
                    lexer->mark_end(lexer);
                    return did_advance;
                }

                if (lexer->lookahead == '{') {
                    lexer->mark_end(lexer);

                    while (lexer->lookahead == '{' && brace_advanced < current_interpolation->open_brace_count) {
                        advance(lexer);
                        brace_advanced++;
                    }

                    if (brace_advanced == current_interpolation->open_brace_count &&
                        (brace_advanced == 0 || lexer->lookahead != '{')) { // if we're in a brace we're not allowed to
                                                                            // collect more than the open_brace_count
                        return did_advance;
                    }
                }
            }

            if (lexer->lookahead != '{') {
                brace_advanced = 0;
            }
            advance(lexer);
            did_advance = true;
        }

        lexer->mark_end(lexer);
        return did_advance;
    }

    return false;
}
