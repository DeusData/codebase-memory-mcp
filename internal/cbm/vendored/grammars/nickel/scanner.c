#include "tree_sitter/parser.h"
#include <assert.h>
#include <stdint.h>
#include <wctype.h>

#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define VEC_RESIZE(vec, _cap)                                                                                          \
    uint8_t *tmp = realloc((vec).data, (_cap) * sizeof((vec).data[0]));                                                   \
    assert(tmp != NULL);                                                                                               \
    (vec).data = tmp;                                                                                                  \
    (vec).cap = (_cap);

#define VEC_GROW(vec, _cap)                                                                                            \
    if ((vec).cap < (_cap)) {                                                                                          \
        VEC_RESIZE((vec), (_cap));                                                                                     \
    }

#define VEC_PUSH(vec, el)                                                                                              \
    if ((vec).cap == (vec).len) {                                                                                      \
        VEC_RESIZE((vec), MAX(16, (vec).len * 2));                                                                     \
    }                                                                                                                  \
    (vec).data[(vec).len++] = (el);

#define VEC_POP(vec) (vec).len--;

#define VEC_NEW                                                                                                        \
    { .len = 0, .cap = 0, .data = NULL }

#define VEC_BACK(vec) ((vec).data[(vec).len - 1])

#define VEC_FREE(vec)                                                                                                  \
    {                                                                                                                  \
        if ((vec).data != NULL)                                                                                        \
            free((vec).data);                                                                                          \
    }

#define VEC_CLEAR(vec) (vec).len = 0;

enum TokenType {
    MULTSTR_START,
    MULTSTR_END,
    STR_START,
    STR_END,
    INTERPOLATION_START,
    INTERPOLATION_END,
    // We use a regular STR_END to close a quoted enum tag, so we just need a
    // start symbol
    QUOTED_ENUM_TAG_START,
    COMMENT,
};

const uint8_t NO_INTERPOLATION = 0;

typedef struct {
    uint32_t len;
    uint32_t cap;
    uint8_t *data;
} vec;

typedef struct {
    vec expected_percent_count;
} Scanner;

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline bool eof(TSLexer *lexer) { return lexer->eof(lexer); }

static inline int32_t lookahead(TSLexer *lexer) { return lexer->lookahead; }

static unsigned serialize(Scanner *scanner, char *buffer) {
    uint8_t size = 0;

    if (scanner->expected_percent_count.len >= TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
        return 0;
    }

    // We serialize to and from uint8_t's, we cannot currently serialize any
    // value above that.
    uint32_t len = scanner->expected_percent_count.len;
    if (len > UINT8_MAX) {
        len = UINT8_MAX;
    }

    buffer[size++] = (char)len;
    for (uint8_t i = 0; i < len; i++) {
        buffer[size++] = (char)scanner->expected_percent_count.data[i];
    }

    return size;
}

static void deserialize(Scanner *scanner, const char *buffer, uint8_t length) {
    // We have a constant size state, so this case should never happen. In
    // case it does, we initialize a fresh state.
    VEC_CLEAR(scanner->expected_percent_count);
    if (length > 0) {
        uint8_t vec_length = buffer[0];

        assert(vec_length + 1 == length);
        for (uint8_t i = 1; i < length; i++) {
            VEC_PUSH(scanner->expected_percent_count, buffer[i]);
        }
    }
}

// Check if `lookahead` is a valid character at the start of a symbolic
// string tag
static bool valid_symtag_start(int32_t lookahead) {
    return ('a' <= lookahead && lookahead <= 'z') || ('A' <= lookahead && lookahead <= 'Z');
}

// Check if `lookahead` is a valid character in the middle of a symbolic
// string tag
static bool valid_symtag_middle(int32_t lookahead) {
    return valid_symtag_start(lookahead) || ('0' <= lookahead && lookahead <= '9') || lookahead == '-' ||
           lookahead == '\'' || lookahead == '_';
}

// Finite state automaton to recognize a symbolic string with a tag,
// matching the regular expression /[a-zA-Z][_a-zA-Z0-9-']*-s%/. If we have
// already consumed an 'm' before calling this function, we set `m_scanned`
// to `true` and the start state of the automaton is changed to MIDDLE.
static bool scan_until_sstr_start_end(TSLexer *lexer, bool m_scanned) {
    enum RecognizerState { START, MIDDLE, DASH, S, PERCENT } state = START;

    if (m_scanned) {
        state = MIDDLE;
    }
    while (!eof(lexer)) {
        int32_t current = lookahead(lexer);
        switch (state) {
            case START:
                if (valid_symtag_start(current)) {
                    advance(lexer);
                    state = MIDDLE;
                } else {
                    return false;
                }
                break;
            case MIDDLE:
                if (!valid_symtag_middle(current)) {
                    return false;
                }
                if (current == '-') {
                    state = DASH;
                }
                advance(lexer);
                break;
            case DASH:
                if (current == 's') {
                    state = S;
                    advance(lexer);
                } else {
                    state = MIDDLE;
                }
                break;
            case S:
                if (current == '%') {
                    state = PERCENT;
                    advance(lexer);
                } else {
                    state = MIDDLE;
                }
                break;
            case PERCENT:
                return true;
        }
    }
    return false; // We ran into EOF without completely scanning a symbolic
                  // string start
}

// Scans a multistring start, either normal (`m%"`) or symbolic
// (`some-prefix-s%"`).
static bool scan_multstr_start(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = MULTSTR_START;

    bool m_scanned = false;

    if (lookahead(lexer) == 'm') {
        advance(lexer);
        m_scanned = true;
    }

    if (m_scanned && lookahead(lexer) == '%') {
        advance(lexer);
    } else if (!scan_until_sstr_start_end(lexer, m_scanned)) {
        return false;
    }

    // At this point we have either scanned `m%` or `some-prefix-s%`, hence
    // `count` should be 1. We now find all following `%` to get the final
    // count.
    uint8_t count = 1;
    while (lookahead(lexer) == '%') {
        count++;
        advance(lexer);
    }

    bool quote = false;
    if (lookahead(lexer) == '"') {
        quote = true;
        advance(lexer);
    }

    VEC_PUSH(scanner->expected_percent_count, count);

    // A MULTSTR_START is fully scanned, if we have gotten to this point and
    // actually found a `"`.
    return quote;
}

// Scans the multistring end. Assumes that the " has already been consumed
static bool scan_multstr_end(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = MULTSTR_END;

    uint8_t count = VEC_BACK(scanner->expected_percent_count);

    // Consume all %-signs
    // count should never be zero here, as we are lexing a multiline string.
    while (lookahead(lexer) == '%' && count > 0) {
        count--;
        advance(lexer);
    }

    VEC_POP(scanner->expected_percent_count);

    // An END is fully scanned when we started with an '"' (precondition of
    // this function) and consumed all %-signs, assuming the next character
    // doesn't indicate the start of an interpolation.
    return count == 0 && lookahead(lexer) != '{';
}

// Precondition of this function is that the lookahead is '"'
static bool scan_str_start(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = STR_START;

    // Interpolation in strings are preceded by a single % sign.
    VEC_PUSH(scanner->expected_percent_count, 1);

    advance(lexer);

    return true;
}

// Precondition of this function is that the lookahead is '"'
static bool scan_str_end(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = STR_END;

    advance(lexer);

    VEC_POP(scanner->expected_percent_count);

    return true;
}

static bool scan_interpolation_start(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = INTERPOLATION_START;

    bool brace = false;
    uint8_t count = VEC_BACK(scanner->expected_percent_count);

    // If count == NO_INTERPOLATION, interpolation is disallowed in the
    // current string (used for quoted enum tag, which must be static
    // strings)
    if (count == NO_INTERPOLATION) {
        return false;
    }

    while (lookahead(lexer) == '%') {
        count--;
        advance(lexer);
    }

    if (lookahead(lexer) == '{') {
        brace = true;
        advance(lexer);
    }

    return brace && (count == 0);
}

// Precondition of this function is that the lookahead is '}'
static bool scan_interpolation_end(TSLexer *lexer) {
    lexer->result_symbol = INTERPOLATION_END;

    advance(lexer);

    return true;
}

// Precondition of this function is that the lookahead is '"'.
static bool scan_quoted_enum_tag_start(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = QUOTED_ENUM_TAG_START;

    // zero is a special value meaning that no interpolation is allowed.
    VEC_PUSH(scanner->expected_percent_count, NO_INTERPOLATION);

    advance(lexer);

    return true;
}

static bool scan_comment(Scanner *scanner, TSLexer *lexer) {
    lexer->result_symbol = COMMENT;

    // The length of the vector gives us the current level of nesting. Thus,
    // an empty vector implies that we are currently not in a string. For
    // anything else, we return false.
    if (scanner->expected_percent_count.len > 0) {
        return false;
    }

    // Consume the #
    advance(lexer);

    while (lookahead(lexer) != '\n' && lookahead(lexer) != '\0') {
        advance(lexer);
    }

    return true;
}

static bool scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    // During error recovery we don't run the external scanner. This
    // produces less accurate results, but avoids a large deal of complexity
    // in this scanner.
    if (valid_symbols[MULTSTR_START] && valid_symbols[MULTSTR_END] && valid_symbols[STR_START] &&
        valid_symbols[STR_END] && valid_symbols[INTERPOLATION_START] && valid_symbols[INTERPOLATION_END] &&
        valid_symbols[COMMENT] && valid_symbols[QUOTED_ENUM_TAG_START]) {
        return false;
    }

    // Skip over all whitespace
    while (iswspace(lookahead(lexer))) {
        skip(lexer);
    }

    switch (lookahead(lexer)) {
        case '"':
            if (valid_symbols[MULTSTR_END]) {
                advance(lexer);
                if (lookahead(lexer) == '%') {
                    return scan_multstr_end(scanner, lexer);
                }
            } else if (valid_symbols[STR_START]) {
                return scan_str_start(scanner, lexer);
            } else if (valid_symbols[STR_END]) {
                return scan_str_end(scanner, lexer);
            }
            break;
        case '%':
            if (valid_symbols[INTERPOLATION_START]) {
                return scan_interpolation_start(scanner, lexer);
            }
            break;
        case '}':
            if (valid_symbols[INTERPOLATION_END]) {
                return scan_interpolation_end(lexer);
            }
            break;
        case '\'':
            if (valid_symbols[QUOTED_ENUM_TAG_START]) {
                advance(lexer);
                if (lookahead(lexer) == '"') {
                    return scan_quoted_enum_tag_start(scanner, lexer);
                }
            }
            break;
        case '#':
            if (valid_symbols[COMMENT]) {
                return scan_comment(scanner, lexer);
            }
            break;
        default:
            if (valid_symbols[MULTSTR_START]) {
                return scan_multstr_start(scanner, lexer);
            }
            break;
    }

    return false;
}

void *tree_sitter_nickel_external_scanner_create() {
    Scanner *scanner = (Scanner *)calloc(1, sizeof(Scanner));
    return scanner;
}

bool tree_sitter_nickel_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)(payload);
    return scan(scanner, lexer, valid_symbols);
}

/**
 * @param Contains the scanner
 * @param Will hold the serialized state of the scanner
 */
unsigned tree_sitter_nickel_external_scanner_serialize(void *payload, char *buffer) {
    Scanner *scanner = (Scanner *)(payload);
    return serialize(scanner, buffer);
}

/**
 * @param Contains the scanner
 * @param The serialised state of the scanner
 * @param Indicates the length of the buffer
 */
void tree_sitter_nickel_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    Scanner *scanner = (Scanner *)(payload);
    deserialize(scanner, buffer, (uint8_t)length);
}

void tree_sitter_nickel_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)(payload);
    VEC_FREE(scanner->expected_percent_count);
    free(scanner);
}
