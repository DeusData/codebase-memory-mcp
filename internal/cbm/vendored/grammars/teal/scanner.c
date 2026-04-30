#include "tree_sitter/parser.h"
#include "tree_sitter/alloc.h"
#include <stdbool.h>
#include <string.h>

typedef struct {
    uint32_t opening_eqs;
    bool in_str;
    char opening_quote;
} State;

void *tree_sitter_teal_external_scanner_create() { return ts_calloc(1, sizeof(State)); }
void tree_sitter_teal_external_scanner_destroy(void *payload) { ts_free(payload); }

static inline void consume(TSLexer *lexer) { lexer->advance(lexer, false); }
static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

enum TokenType {
    COMMENT,

    LONG_STRING_START,
    LONG_STRING_CHAR,
    LONG_STRING_END,

    SHORT_STRING_START,
    SHORT_STRING_CHAR,
    SHORT_STRING_END,
};

#define EXPECT(char) do { if (lexer->lookahead != char) { return false; } consume(lexer); } while (0)
static inline uint32_t consume_eqs(TSLexer *lexer) {
    uint32_t result = 0;
    while (!lexer->eof(lexer) && lexer->lookahead == '=') {
        consume(lexer);
        result += 1;
    }
    return result;
}

static void consume_rest_of_line(TSLexer *lexer) {
    while (!lexer->eof(lexer)) {
        switch (lexer->lookahead) {
            case '\n': case '\r': return;
            default: consume(lexer);
        }
    }
}

static bool scan_comment(TSLexer *lexer) {
    EXPECT('-'); EXPECT('-');
    lexer->result_symbol = COMMENT;

    if (lexer->lookahead != '[') {
        consume_rest_of_line(lexer);
        return true;
    }

    consume(lexer);
    uint32_t eqs = consume_eqs(lexer);

    if (lexer->lookahead != '[') {
        consume_rest_of_line(lexer);
        return true;
    }

    while (!lexer->eof(lexer)) {
        while (!lexer->eof(lexer) && lexer->lookahead != ']')
            consume(lexer);

        EXPECT(']');
        uint32_t test_eq = consume_eqs(lexer);
        if (lexer->lookahead == ']') {
            consume(lexer);
            if (test_eq == eqs) {
                return true;
            }
        } else if (!lexer->eof(lexer)) {
            consume(lexer);
        }
    }

    return true;
}

static inline void reset_state(State *state) {
    *state = (State) {
        .opening_eqs = 0,
        .in_str = false,
        .opening_quote = 0,
    };
}

unsigned tree_sitter_teal_external_scanner_serialize(void *payload, char *buffer) {
    memcpy(buffer, payload, sizeof(State));
    return sizeof(State);
}

void tree_sitter_teal_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    if (length < sizeof(State))
        return;
    memcpy(payload, buffer, sizeof(State));
}

static bool scan_short_string_start(State *state, TSLexer *lexer) {
    if ((lexer->lookahead == '"') || (lexer->lookahead == '\'')) {
        state->opening_quote = (char)lexer->lookahead;
        state->in_str = true;
        consume(lexer);
        lexer->result_symbol = SHORT_STRING_START;
        return true;
    }
    return false;
}

static bool scan_short_string_end(State *state, TSLexer *lexer) {
    if (state->in_str && lexer->lookahead == state->opening_quote) {
        consume(lexer);
        lexer->result_symbol = SHORT_STRING_END;
        reset_state(state);
        return true;
    }
    return false;
}

static bool scan_short_string_char(State *state, TSLexer *lexer) {
    if (
        state->in_str
        && state->opening_quote > 0
        && lexer->lookahead != state->opening_quote
        && lexer->lookahead != '\n'
        && lexer->lookahead != '\r'
        && lexer->lookahead != '\\'
        && lexer->lookahead != '%'
    ) {
        consume(lexer);
        lexer->result_symbol = SHORT_STRING_CHAR;
        return true;
    }
    return false;
}

static bool scan_long_string_start(State *state, TSLexer *lexer) {
    EXPECT('[');
    reset_state(state);
    uint32_t eqs = consume_eqs(lexer);
    EXPECT('[');
    state->in_str = true;
    lexer->result_symbol = LONG_STRING_START;
    state->opening_eqs = eqs;
    return true;
}

static bool scan_long_string_end(State *state, TSLexer *lexer) {
    EXPECT(']');

    uint32_t eqs = consume_eqs(lexer);
    if (state->opening_eqs == eqs && lexer->lookahead == ']') {
        consume(lexer);
        lexer->result_symbol = LONG_STRING_END;
        reset_state(state);
        return true;
    }
    return false;
}

static bool scan_long_string_char(TSLexer *lexer) {
    if (lexer->lookahead == '%') {
        return false;
    }
    consume(lexer);
    lexer->result_symbol = LONG_STRING_CHAR;
    return true;
}

static inline bool is_ascii_whitespace(uint32_t chr) {
    switch (chr) {
        default: return false;
        case '\n': case '\r': case ' ': case '\t':
            return true;
    }
}

bool tree_sitter_teal_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    State *state = payload;
    if (lexer->eof(lexer))
        return false;

    if (state->in_str) {
        if (state->opening_quote > 0) {
            return (valid_symbols[SHORT_STRING_END] && scan_short_string_end(state, lexer))
                || (valid_symbols[SHORT_STRING_CHAR] && scan_short_string_char(state, lexer));
        }
        return scan_long_string_end(state, lexer) || scan_long_string_char(lexer);
    }

    while (is_ascii_whitespace(lexer->lookahead))
        skip(lexer);

    if (valid_symbols[SHORT_STRING_START] && scan_short_string_start(state, lexer))
        return true;

    if (valid_symbols[LONG_STRING_START] && scan_long_string_start(state, lexer))
        return true;

    return valid_symbols[COMMENT] && scan_comment(lexer);
}

