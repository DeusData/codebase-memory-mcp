#include "tree_sitter/alloc.h"
#include "tree_sitter/parser.h"
#include <ctype.h>
#include <wctype.h>
#include <string.h>

enum TokenType {
    LINE_CONTINUATION,
    INTEGER_LITERAL,
    FLOAT_LITERAL,
    BOZ_LITERAL,
    STRING_LITERAL,
    STRING_LITERAL_KIND,
    END_OF_STATEMENT,
    PREPROC_UNARY_OPERATOR,
    HOLLERITH_CONSTANT,
    DO_LABEL,
    DO_LABEL_VIRTUAL,
    DO_LABEL_CONTINUE,
};

// at most 100 nested labeled do loops, should be sufficient
#define MAX_LABEL_STACK 100

typedef struct {
    bool in_line_continuation;

    // stack for tracking active DO labels and their counts
    int32_t depth;
    int32_t labels[MAX_LABEL_STACK];
    int32_t counts[MAX_LABEL_STACK];

    // counter for emitting virtual label token for closing labeled do statements
    int32_t pending_label_virtual;
    // flag for emitting eos tokens right after a virtual label token
    bool is_pending_eos_virtual;
} Scanner;


// first parse number and only then decide what kind of token to emit,
// for example: we need to check label stack first before we can decide whether
// we have a do-label or an integer literal
typedef enum {
    NUMBER_NONE,
    NUMBER_INTEGER,
    NUMBER_FLOAT
} NumberType;

typedef struct {
    // type of number
    NumberType type;
    // value in case it turns out to be a label
    int32_t value;
    // count digits to abort value computation after 5 digits
    // (maximal number for labels)
    int digit_count;
} NumberResult;

//  consume current character into current token and advance
static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

// ignore current character and advance
static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

// is `chr` ok for an identifier?
static bool is_identifier_char(char chr) { return iswalnum(chr) || chr == '_'; }

static bool is_boz_sentinel(char chr) {
    switch (chr) {
    case 'B':
    case 'b':
    case 'O':
    case 'o':
    case 'Z':
    case 'z':
        return true;
    default:
        return false;
    }
}

static bool is_exp_sentinel(char chr) {
    switch (chr) {
    case 'D':
    case 'd':
    case 'E':
    case 'e':
    case 'Q':
    case 'q':
        return true;
    default:
        return false;
    }
}

// If in the middle of a literal, '&' is required in both lines
static bool skip_literal_continuation_sequence(TSLexer *lexer) {
    if (lexer->lookahead != '&') {
        return true;
    }

    advance(lexer);
    while (iswspace(lexer->lookahead)) {
        advance(lexer);
    }
    // second '&' technically required to continue the literal
    if (lexer->lookahead == '&') {
        advance(lexer);
        return true;
    }
    return false;
}

// consume digits and compute value if requested
static bool scan_int(TSLexer *lexer, int32_t *value, int *count) {
    if (!iswdigit(lexer->lookahead)) {
        return false;
    }

    if (value && count) {
        *value = 0;
        *count = 0;
    }

    // outer loop for handling line continuations
    while (true) {
        // consume digits
        while (iswdigit(lexer->lookahead)) {
            if (value && count && *count < 7) {
                *value = *value * 10 + (lexer->lookahead - '0');
                (*count)++;
            }
            advance(lexer);
        }
        lexer->mark_end(lexer);

        if (lexer->lookahead == '&') {
            // with the lookahead check above, it returns true if in a continuation
            // line and consumes both & and whitespace characters inbetween
            if (skip_literal_continuation_sequence(lexer)) {
                continue;
            }
        }

        // no digits and no continuation found
        break;
    }

    return true;
}

/// Scan integer or float of the forms 1XXX, 1.0XXX, 0.1XXX, 1.XDX, .1X etc.
static NumberResult scan_number(TSLexer *lexer) {
    NumberResult result = {NUMBER_NONE, 0, 0};

    // assume integer, but if no proper digits are found, reset to NUMBER_NONE
    result.type = NUMBER_INTEGER;

    // collect initial digits and compute value (specifically required to
    // determine label value)
    bool digits = scan_int(lexer, &result.value, &result.digit_count);

    if (lexer->lookahead == '.') {
        advance(lexer);
        // exclude decimal if followed by any letter other than d/D and e/E
        // if no leading digits are present and a non-digit follows
        // the decimal it's a nonmatch.
        if (digits && !iswalnum(lexer->lookahead)) {
            lexer->mark_end(lexer); // add decimal to token
        }
        // this is not yet decided, we still need to find some digits
        result.type = NUMBER_FLOAT;
    }

    // if next char isn't number return since we handle exp
    // notation and precision identifiers separately. If there are
    // no leading digit it's a nonmatch.
    digits = scan_int(lexer, NULL, NULL) || digits;

    if (digits) {
        // process exp notation
        if (is_exp_sentinel(lexer->lookahead)) {
            advance(lexer);
            if (lexer->lookahead == '+' || lexer->lookahead == '-') {
                advance(lexer);
                lexer->mark_end(lexer);
            }
            if (!scan_int(lexer, NULL, NULL)) {
                result.type = NUMBER_INTEGER;
                return result; // valid number token with junk after it
            }
            result.type = NUMBER_FLOAT;
        }
    }

    if (!digits) {
        result.type = NUMBER_NONE;
    }
    return result;
}

static bool scan_boz(TSLexer *lexer) {
    lexer->result_symbol = BOZ_LITERAL;
    bool boz_prefix = false;
    char quote = '\0';
    if (is_boz_sentinel(lexer->lookahead)) {
        advance(lexer);
        boz_prefix = true;
    }
    if (lexer->lookahead == '\'' || lexer->lookahead == '"') {
        quote = lexer->lookahead;
        advance(lexer);
        if (!iswxdigit(lexer->lookahead)) {
            return false;
        }
        while (iswxdigit(lexer->lookahead)) {
            advance(lexer); // store all hex digits
        }
        if (lexer->lookahead != quote) {
            return false;
        }
        advance(lexer); // store enclosing quote
        if (!boz_prefix && !is_boz_sentinel(lexer->lookahead)) {
            return false; // no boz suffix or prefix provided
        }
        lexer->mark_end(lexer);
        return true;
    }
    return false;
}

/// Need to dynamically determining the length of the Hollerith constant
static bool scan_hollerith_constant(TSLexer *lexer) {
    // Try to parse nH<text> where n is the number of characters in <text>

    // Read integer prefix 'n'
    unsigned length = 0;
    while (iswdigit(lexer->lookahead)) {
        unsigned new_length = length * 10 + (lexer->lookahead - '0');
        // The number of characters has no limit but overflow has to be handled
        if (new_length < length) {
            return false;
        }
        length = new_length;
        advance(lexer);

        if (!skip_literal_continuation_sequence(lexer)) {
            return false;
        }
    }

    // 0 is invalid 'n' in Hollerith constants
    if (length == 0) {
        return false;
    }

    // Expect 'H' or 'h'
    if (lexer->lookahead != 'H' && lexer->lookahead != 'h') {
        return false;
    }
    advance(lexer);

    // Read exactly 'n' characters
    for (unsigned i = 0; i < length; i++) {
        if (!lexer->lookahead || lexer->eof(lexer)) {
            return false;
        }
        if (!skip_literal_continuation_sequence(lexer)) {
            return false;
        }
        advance(lexer);
    }
    lexer->result_symbol = HOLLERITH_CONSTANT;
    lexer->mark_end(lexer);
    return true;
}

static bool scan_end_of_statement(Scanner *scanner, TSLexer *lexer) {
    // Things that end statements in Fortran:
    //
    // - semicolons
    // - end-of-line (various representations)
    // - comments
    //
    // Comments are a bit surprising, but it turns out to be
    // easier to handle line continuations if comments consume the
    // newline

    // Semicolons and EOF always end the statement
    if (lexer->eof(lexer)) {
        skip(lexer);
        lexer->result_symbol = END_OF_STATEMENT;
        return true;
    }

    // If we're in a line continuation, then don't end the statement
    if (scanner->in_line_continuation) {
        return false;
    }

    // Consume end of line characters, we allow '\n', '\r\n' and
    // '\r' to cover unix, MSDOS and old style Macintosh.
    // Handle comments here too, but don't consume them
    if (lexer->lookahead == '\r') {
        skip(lexer);
        if (lexer->lookahead == '\n') {
            skip(lexer);
        }
    } else {
        if (lexer->lookahead == '\n') {
            skip(lexer);
        } else if (lexer->lookahead != '!') {
            // Not a newline and not a comment, so not an
            // end-of-statement
            return false;
        }
    }

    lexer->result_symbol = END_OF_STATEMENT;
    return true;
}

static bool scan_start_line_continuation(Scanner *scanner, TSLexer *lexer) {
    // Now see if we should start a line continuation
    scanner->in_line_continuation = (lexer->lookahead == '&');
    if (!scanner->in_line_continuation) {
        return false;
    }
    // Consume the '&'
    advance(lexer);
    lexer->result_symbol = LINE_CONTINUATION;
    return true;
}

static bool scan_end_line_continuation(Scanner *scanner, TSLexer *lexer) {
    if (!scanner->in_line_continuation) {
        return false;
    }
    // Everything except comments ends a line continuation
    if (lexer->lookahead == '!') {
        return false;
    }

    scanner->in_line_continuation = false;

    // Consume any leading line continuation markers
    if (lexer->lookahead == '&') {
        advance(lexer);
    }
    lexer->result_symbol = LINE_CONTINUATION;
    return true;
}

static bool scan_string_literal_kind(TSLexer *lexer) {
    // Strictly, it's allowed for the kind to be an integer literal, in
    // practice I've not seen it
    if (!iswalpha(lexer->lookahead)) {
        return false;
    }

    lexer->result_symbol = STRING_LITERAL_KIND;

    // We need two characters of lookahead to see `_"`
    char current_char = '\0';

    while (is_identifier_char(lexer->lookahead) && !lexer->eof(lexer)) {
        current_char = lexer->lookahead;
        // Don't capture the trailing underscore as part of the kind identifier
        if (lexer->lookahead == '_') {
            lexer->mark_end(lexer);
        }
        advance(lexer);
    }

    if ((current_char != '_') || (lexer->lookahead != '"' && lexer->lookahead != '\'')) {
        return false;
    }

    return true;
}

static bool scan_string_literal(TSLexer *lexer) {
    const char opening_quote = lexer->lookahead;

    if (opening_quote != '"' && opening_quote != '\'') {
        return false;
    }

    advance(lexer);
    lexer->result_symbol = STRING_LITERAL;

    while (lexer->lookahead != '\n' && !lexer->eof(lexer)) {
        // Handle line continuations: strictly speaking, we MUST have
        // both trailing '&' on first line AND leading '&' on second
        // line, though most compilers do accept string literals
        // missing the second '&'. In practice, everyone does seem to
        // include it.

        // We need to handle this here because sometimes '&' is part
        // of the literal and not a continuation marker, and otherwise
        // the parser gets confused, especially if there's no
        // whitespace before the '&' in the string

        // The literal token will end up containing the line
        // continuation as well as any blank or comment lines inside
        // the quotes (yes, you can have comments _inside_ string
        // literals if they contain a line continuation)
        if (lexer->lookahead == '&') {
            advance(lexer);
            // Consume blanks up to the end of the line or non-blank
            while (iswblank(lexer->lookahead)) {
                advance(lexer);
            }
            // If we hit the end of the line, consume all whitespace,
            // including new lines
            if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
                while (iswspace(lexer->lookahead)) {
                    advance(lexer);
                }
            }
            continue;
        }

        // If we hit the same kind of quote that opened this literal,
        // check to see if there's two in a row, and if so, consume
        // both of them
        if (lexer->lookahead == opening_quote) {
            advance(lexer);
            // It was just one quote, so we've successfully reached
            // the end of the literal. We also need to check that an
            // escaped quote isn't split in half by a line
            // continuation -- people do this!
            lexer->mark_end(lexer);
            skip_literal_continuation_sequence(lexer);
            if (lexer->lookahead != opening_quote) {
                return true;
            }
        }
        advance(lexer);
    }

    // We hit the end of the line without an '&', so this is an
    // unclosed string literal (an error)
    return false;
}

/// Need an external scanner to catch '!' before its parsed as a comment
static bool scan_preproc_unary_operator(TSLexer *lexer) {
    const char next_char = lexer->lookahead;
    if (next_char == '!' || next_char == '~' || next_char == '-' || next_char == '+') {
        advance(lexer);
        lexer->result_symbol = PREPROC_UNARY_OPERATOR;
        return true;
    }
    return false;
}


static void track_labeled_do(Scanner *scanner, int32_t label) {
    // check if label already exists
    int i = scanner->depth - 1;
    if (scanner->labels[i] == label) {
        scanner->counts[i]++;
        return;
    }

    // not at top of stack, assume new label, add it to stack
    if (scanner->depth < MAX_LABEL_STACK) {
        scanner->labels[scanner->depth] = label;
        scanner->counts[scanner->depth] = 1;
        scanner->depth++;
    } else {
        // should we properly abort here?
    }
}

// check whether an end of statement token for virtual do labels is pending,
// emit END_OF_STATEMENT and update internal state accordingly
static inline bool scan_do_label_eos(Scanner *scanner, TSLexer *lexer) {
    if (scanner->is_pending_eos_virtual) {
        scanner->is_pending_eos_virtual = false;
        lexer->result_symbol = END_OF_STATEMENT;
        return true;
    } else {
        return false;
    }
}

// check whether do labels are pending, emit DO_LABEL_VIRTUAL or DO_LABEL_CONTINUE
// and update internal state accordingly
static inline bool scan_do_label_pending(Scanner *scanner, TSLexer *lexer) {
    if (scanner->pending_label_virtual > 0) {
        if (scanner->pending_label_virtual > 1) {
            scanner->pending_label_virtual--;
            // schedule an eos for the next token to finish the virtual statement
            scanner->is_pending_eos_virtual = true;
            lexer->result_symbol = DO_LABEL_VIRTUAL;
        } else {
            // emit last termination symbol which is do_label_continue
            scanner->pending_label_virtual = 0;
            lexer->result_symbol = DO_LABEL_CONTINUE;
        }
        return true;
    } else {
        return false;
    }
}

// only invoked after parser has found a "do" (and thus DO_LABEL is a valid
// token) and the scan has consumed a proper integer value provided as label
static void scan_do_label(Scanner *scanner, TSLexer *lexer, int32_t label) {
    track_labeled_do(scanner, label);
    lexer->result_symbol = DO_LABEL;
}

static bool scan_do_label_continue(Scanner *scanner, TSLexer *lexer, int32_t label) {
    // determine whether this label belongs to the last labeled do,
    // if it does, remove it from stack and determine how many loops it closes
    int32_t loops_to_close = 0;
    if (scanner->depth > 0) {
        int i = scanner->depth - 1;
        if (scanner->labels[i] == label) {
            loops_to_close = scanner->counts[i];
            // remove from stack
            scanner->depth--;
        }
    }

    // scanner->counts[i] is always greater than zero, if the label is on the stack,
    // hence loops_to_close == 0 means depth=0 or label is not at top of stack
    if (loops_to_close == 0) {
        // label not on stack, hence this does not close a labeled do
        return false;
    }

    scanner->pending_label_virtual = loops_to_close;
    scanner->is_pending_eos_virtual = false;
    scan_do_label_pending(scanner, lexer);
    return true;
}

// check for label, number of boz token
static bool scan_label_number_boz(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    // extract out root number from expression (without kind if present)
    NumberResult result = scan_number(lexer);

    // check for a do-label, should have at most 5 digits and DO_LABEL
    // or DO_LABEL_CONTINUE are valid symbols
    if (result.type == NUMBER_INTEGER && result.digit_count < 6) {
        if (valid_symbols[DO_LABEL]) {
            scan_do_label(scanner, lexer, result.value);
            return true;
        }
        if (valid_symbols[DO_LABEL_CONTINUE] &&
            scan_do_label_continue(scanner, lexer, result.value)) {
            return true;
        }
    }

    // not a label
    if (result.type == NUMBER_INTEGER) {
        lexer->result_symbol = INTEGER_LITERAL;
        return true;
    } else if (result.type == NUMBER_FLOAT) {
        lexer->result_symbol = FLOAT_LITERAL;
        return true;
    }

    if (scan_boz(lexer)) {
        return true;
    }

    return false;
}

static bool scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    // handle pending virtual labels and eos first
    if (valid_symbols[END_OF_STATEMENT]) {
        if (scan_do_label_eos(scanner, lexer)) {
            return true;
        }
    }

    if(valid_symbols[DO_LABEL_CONTINUE] || valid_symbols[DO_LABEL_VIRTUAL]) {
        if (scan_do_label_pending(scanner, lexer)) {
            return true;
        }
    }

    // Consume any leading whitespace except newlines
    while (iswblank(lexer->lookahead)) {
        skip(lexer);
    }

    // Close the current statement if we can
    if (valid_symbols[END_OF_STATEMENT]) {
        if (scan_end_of_statement(scanner, lexer)) {
            return true;
        }
    }

    // We're now either in a line continuation or between
    // statements, so we should eat all whitespace including
    // newlines, until we come to something more interesting
    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }

    if (scan_end_line_continuation(scanner, lexer)) {
        return true;
    }

    if (valid_symbols[STRING_LITERAL]) {
        if (scan_string_literal(lexer)) {
            return true;
        }
    }

    if (valid_symbols[HOLLERITH_CONSTANT]) {
        if (scan_hollerith_constant(lexer)) {
            return true;
        }
    }

    if (valid_symbols[INTEGER_LITERAL] ||
        valid_symbols[FLOAT_LITERAL] ||
        valid_symbols[BOZ_LITERAL] ||
        valid_symbols[DO_LABEL] ||
        valid_symbols[DO_LABEL_CONTINUE]) {
        if (scan_label_number_boz(scanner, lexer, valid_symbols)) {
            return true;
        }
    }

    if (valid_symbols[PREPROC_UNARY_OPERATOR]) {
        if (scan_preproc_unary_operator(lexer)) {
            return true;
        }
    }

    if (scan_start_line_continuation(scanner, lexer)) {
        return true;
    }

    if (valid_symbols[STRING_LITERAL_KIND]) {
        // This may need a lot of lookahead, so should (probably) always
        // be the last token to look for
        if (scan_string_literal_kind(lexer)) {
            return true;
        }
    }

    return false;
}

void *tree_sitter_fortran_external_scanner_create() {
    Scanner *scanner = ts_calloc(1, sizeof(Scanner));
    scanner->in_line_continuation = false;
    scanner->depth = 0;
    scanner->pending_label_virtual = 0;
    scanner->is_pending_eos_virtual = false;
    return scanner;
}

bool tree_sitter_fortran_external_scanner_scan(void *payload, TSLexer *lexer,
                                               const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;
    return scan(scanner, lexer, valid_symbols);
}

unsigned tree_sitter_fortran_external_scanner_serialize(void *payload,
                                                        char *buffer) {
    Scanner *scanner = (Scanner *)payload;

    if (scanner->depth > MAX_LABEL_STACK) return 0;

    size_t size = 0;

    buffer[size] = (char)scanner->in_line_continuation;
    size += 1;

    memcpy(&buffer[size], &scanner->depth, sizeof(int32_t));
    size += sizeof(int32_t);

    memcpy(&buffer[size], scanner->labels, scanner->depth * sizeof(int32_t));
    size += scanner->depth * sizeof(int32_t);

    memcpy(&buffer[size], scanner->counts, scanner->depth * sizeof(int32_t));
    size += scanner->depth * sizeof(int32_t);

    memcpy(&buffer[size], &scanner->pending_label_virtual, sizeof(int32_t));
    size += sizeof(int32_t);

    buffer[size] = (char)scanner->is_pending_eos_virtual;
    size += 1;

    return size;
}

void tree_sitter_fortran_external_scanner_deserialize(void *payload,
                                                      const char *buffer,
                                                      unsigned length) {
    Scanner *scanner = (Scanner *)payload;

    if (length == 0) {
        scanner->in_line_continuation = false;
        scanner->depth = 0;
        scanner->pending_label_virtual = 0;
        scanner->is_pending_eos_virtual = false;
        return;
    }

    size_t size = 0;

    scanner->in_line_continuation = buffer[size];
    size += 1;

    memcpy(&scanner->depth, &buffer[size], sizeof(int32_t));
    size += sizeof(int32_t);

    if (scanner->depth > MAX_LABEL_STACK) {
        scanner->depth = 0;
        scanner->pending_label_virtual = 0;
        scanner->is_pending_eos_virtual = false;
        return;
    }

    memcpy(scanner->labels, &buffer[size], scanner->depth * sizeof(int32_t));
    size += scanner->depth * sizeof(int32_t);

    memcpy(scanner->counts, &buffer[size], scanner->depth * sizeof(int32_t));
    size += scanner->depth * sizeof(int32_t);

    memcpy(&scanner->pending_label_virtual, &buffer[size], sizeof(int32_t));
    size += sizeof(int32_t);

    scanner->is_pending_eos_virtual = buffer[size];
    size += 1;
}

void tree_sitter_fortran_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    ts_free(scanner);
}