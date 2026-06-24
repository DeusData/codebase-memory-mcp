#include "tree_sitter/parser.h"

#include <stdio.h>
#include <wctype.h>

enum TokenType {
    AUTOMATIC_SEMICOLON,
    TEMPLATE_CHARS,
    TERNARY_QMARK,
    ELVIS_OPERATOR,
    LOGICAL_OR,
    REGEX_PATTERN,
    QUERY_TEXT,
    TAG_LINEFEED,
    CFML_TEMPLATE_CONTENT
};

void *tree_sitter_cfscript_external_scanner_create() { return NULL; }

void tree_sitter_cfscript_external_scanner_destroy(void *p) {}

unsigned tree_sitter_cfscript_external_scanner_serialize(void *payload, char *buffer) { return 0; }

void tree_sitter_cfscript_external_scanner_deserialize(void *p, const char *b, unsigned n) {}

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static bool scan_template_chars(TSLexer *lexer) {
    lexer->result_symbol = TEMPLATE_CHARS;
    for (bool has_content = false;; has_content = true) {
        lexer->mark_end(lexer);
        switch (lexer->lookahead) {
            case '`':
                return has_content;
            case '\0':
                return false;
            case '$':
                advance(lexer);
                if (lexer->lookahead == '{') {
                    return has_content;
                }
                break;
            case '\\':
                return has_content;
            default:
                advance(lexer);
        }
    }
}

typedef enum {
    REJECT,     // Semicolon is illegal, ie a syntax error occurred
    NO_NEWLINE, // Unclear if semicolon will be legal, continue
    ACCEPT,     // Semicolon is legal, assuming a comment was encountered
} WhitespaceResult;

/**
 * @param consume If false, only consume enough to check if comment indicates semicolon-legality
 */
static WhitespaceResult scan_whitespace_and_comments(TSLexer *lexer, bool *scanned_comment, bool consume) {
    bool saw_block_newline = false;

    for (;;) {
        while (iswspace(lexer->lookahead) ) {
            skip(lexer);
        }

        if (lexer->lookahead == '/') {
            skip(lexer);

            if (lexer->lookahead == '/') {
                skip(lexer);
                while (lexer->lookahead != 0 && lexer->lookahead != '\n' && lexer->lookahead != 0x2028 &&
                       lexer->lookahead != 0x2029) {
                    skip(lexer);
                }
                *scanned_comment = true;
            } else if (lexer->lookahead == '*') {
                skip(lexer);
                while (lexer->lookahead != 0) {
                    if (lexer->lookahead == '*') {
                        skip(lexer);
                        if (lexer->lookahead == '/') {
                            skip(lexer);
                            *scanned_comment = true;

                            if (lexer->lookahead != '/' && !consume) {
                                return saw_block_newline ? ACCEPT : NO_NEWLINE;
                            }

                            break;
                        }
                    } else if (lexer->lookahead == '\n' || lexer->lookahead == 0x2028 || lexer->lookahead == 0x2029) {
                        saw_block_newline = true;
                        skip(lexer);
                    } else {
                        skip(lexer);
                    }
                }
            } else {
                return REJECT;
            }
        } else {
            return ACCEPT;
        }
    }
}

// Check if the current position matches a CFML word operator (case-insensitive).
// Returns true if a word operator is found, false otherwise.
static bool scan_cfml_word_operator(TSLexer *lexer) {
    // Collect up to 10 chars to identify the operator
    char buf[11] = {0};
    int len = 0;
    for (; len < 10 && iswalpha(lexer->lookahead); len++) {
        buf[len] = towlower(lexer->lookahead);
        skip(lexer);
    }
    bool at_end = !iswalnum(lexer->lookahead);
    if (!at_end) return false;

    // Match against known word operators
    return (len == 2 && (
        (buf[0] == 'o' && buf[1] == 'r') ||
        (buf[0] == 'e' && buf[1] == 'q') ||
        (buf[0] == 'g' && buf[1] == 't') ||
        (buf[0] == 'g' && buf[1] == 'e') ||
        (buf[0] == 'l' && buf[1] == 't') ||
        (buf[0] == 'l' && buf[1] == 'e') ||
        (buf[0] == 'i' && buf[1] == 'n')
    )) || (len == 3 && (
        (buf[0] == 'a' && buf[1] == 'n' && buf[2] == 'd') ||
        (buf[0] == 'n' && buf[1] == 'e' && buf[2] == 'q') ||
        (buf[0] == 'n' && buf[1] == 'o' && buf[2] == 't') ||
        (buf[0] == 'g' && buf[1] == 't' && buf[2] == 'e') ||
        (buf[0] == 'l' && buf[1] == 't' && buf[2] == 'e') ||
        (buf[0] == 'm' && buf[1] == 'o' && buf[2] == 'd')
    )) || (len == 10 &&
        buf[0] == 'i' && buf[1] == 'n' && buf[2] == 's' && buf[3] == 't' &&
        buf[4] == 'a' && buf[5] == 'n' && buf[6] == 'c' && buf[7] == 'e' &&
        buf[8] == 'o' && buf[9] == 'f'
    );
}

static bool scan_automatic_semicolon(TSLexer *lexer, bool comment_condition, bool *scanned_comment) {
    lexer->result_symbol = AUTOMATIC_SEMICOLON;
    lexer->mark_end(lexer);

    for (;;) {
        if (lexer->lookahead == 0) {
            return true;
        }

        if (lexer->lookahead == '/') {
            WhitespaceResult result = scan_whitespace_and_comments(lexer, scanned_comment, false);
            if (result == REJECT) {
                return false;
            }

            if (result == ACCEPT && comment_condition && lexer->lookahead != ',' && lexer->lookahead != '=') {
                return true;
            }
        }

        if (lexer->lookahead == '}') {
            return true;
        }

        if (lexer->is_at_included_range_start(lexer)) {
            return true;
        }

        if (lexer->lookahead == '\n' || lexer->lookahead == 0x2028 || lexer->lookahead == 0x2029) {
            break;
        }

        if (!iswspace(lexer->lookahead)) {
            return false;
        }

        skip(lexer);
    }

    skip(lexer);

    if (scan_whitespace_and_comments(lexer, scanned_comment, true) == REJECT) {
        return false;
    }

    switch (lexer->lookahead) {
        case '`':
        case ',':
        case ':':
        case ';':
        case '*':
        case '%':
        case '>':
        case '<':
        case '=':
        case '[':
        case '(':
        case '?':
        case '^':
        case '|':
        case '&':
        case '/':
            return false;

        // Insert a semicolon before decimals literals but not otherwise.
        case '.':
            skip(lexer);
            return iswdigit(lexer->lookahead);

        // Insert a semicolon before `--` and `++`, but not before binary `+` or `-`.
        case '+':
            skip(lexer);
            return lexer->lookahead == '+';
        case '-':
            skip(lexer);
            return lexer->lookahead == '-';

        // Don't insert a semicolon before `!=`, but do insert one before a unary `!`.
        case '!':
            skip(lexer);
            return lexer->lookahead != '=';

        // Don't insert a semicolon before CFML word operators
        // (and, or, eq, neq, not, gt, gte, ge, lt, lte, le, mod, in, instanceof)
        case 'a': case 'A':
        case 'o': case 'O':
        case 'e': case 'E':
        case 'n': case 'N':
        case 'g': case 'G':
        case 'l': case 'L':
        case 'm': case 'M':
        case 'i':
            return !scan_cfml_word_operator(lexer);

        default:
            break;
    }

    return true;
}

static bool scan_ternary_qmark(TSLexer *lexer) {
    for (;;) {
        if (!iswspace(lexer->lookahead)) {
            break;
        }
        skip(lexer);
    }

    if (lexer->lookahead == '?') {
        advance(lexer);

        // Skip whitespace between ? and : for elvis operator
        while (lexer->lookahead == ' ' || lexer->lookahead == '\t') {
            advance(lexer);
        }

        if (lexer->lookahead == ':') {

            advance(lexer);
            lexer->mark_end(lexer);
            lexer->result_symbol = ELVIS_OPERATOR;

            return true;
        
        } else if (lexer->lookahead == '?') {
            return false;
        }

        lexer->mark_end(lexer);
        lexer->result_symbol = TERNARY_QMARK;

        if (lexer->lookahead == '.') {
            advance(lexer);
            if (iswdigit(lexer->lookahead)) {
                return true;
            }
            return false;
        }
        return true;
    }
    return false;
}

/*
static bool scan_html_comment(TSLexer *lexer) {
    while (iswspace(lexer->lookahead) || lexer->lookahead == 0x2028 || lexer->lookahead == 0x2029) {
        skip(lexer);
    }

    const char *comment_start = "<!--";
    const char *comment_end = "-->";

    if (lexer->lookahead == '<') {
        for (unsigned i = 0; i < 4; i++) {
            if (lexer->lookahead != comment_start[i]) {
                return false;
            }
            advance(lexer);
        }
    } else if (lexer->lookahead == '-') {
        for (unsigned i = 0; i < 3; i++) {
            if (lexer->lookahead != comment_end[i]) {
                return false;
            }
            advance(lexer);
        }
    } else {
        return false;
    }

    while (lexer->lookahead != 0 && lexer->lookahead != '\n' && lexer->lookahead != 0x2028 &&
           lexer->lookahead != 0x2029) {
        advance(lexer);
    }

    lexer->result_symbol = HTML_COMMENT;
    lexer->mark_end(lexer);

    return true;
}
    */

    /*
static bool scan_jsx_text(TSLexer *lexer) {
    // saw_text will be true if we see any non-whitespace content, or any whitespace content that is not a newline and
    // does not immediately follow a newline.
    bool saw_text = false;
    // at_newline will be true if we are currently at a newline, or if we are at whitespace that is not a newline but
    // immediately follows a newline.
    bool at_newline = false;

    while (lexer->lookahead != 0 && lexer->lookahead != '<' && lexer->lookahead != '>' && lexer->lookahead != '{' &&
           lexer->lookahead != '}' && lexer->lookahead != '&') {
        bool is_wspace = iswspace(lexer->lookahead);
        if (lexer->lookahead == '\n') {
            at_newline = true;
        } else {
            // If at_newline is already true, and we see some whitespace, then it must stay true.
            // Otherwise, it should be false.
            //
            // See the table below to determine the logic for computing `saw_text`.
            //
            // |------------------------------------|
            // | at_newline | is_wspace | saw_text  |
            // |------------|-----------|-----------|
            // | false (0)  | false (0) | true  (1) |
            // | false (0)  | true  (1) | true  (1) |
            // | true  (1)  | false (0) | true  (1) |
            // | true  (1)  | true  (1) | false (0) |
            // |------------------------------------|

            at_newline &= is_wspace;
            if (!at_newline) {
                saw_text = true;
            }
        }

        advance(lexer);
    }

    lexer->result_symbol = JSX_TEXT;
    return saw_text;
}
*/

static bool scan_query_text(TSLexer *lexer) {
    
    bool saw_text = false;
    bool at_newline = false;

    while (lexer->lookahead != '"') {
        bool is_wspace = iswspace(lexer->lookahead);
        if (lexer->lookahead == '\n') {
            at_newline = true;
        } else {
            at_newline &= is_wspace;
            if (!at_newline) {
                saw_text = true;
            }
        }
        advance(lexer);
    }

    lexer->result_symbol = QUERY_TEXT;
    return saw_text;
}

static bool scan_cfml_template_content(TSLexer *lexer) {
    lexer->result_symbol = CFML_TEMPLATE_CONTENT;
    bool has_content = false;
    while (lexer->lookahead != 0) {
        if (lexer->lookahead == '`') {
            lexer->mark_end(lexer);
            advance(lexer);
            if (lexer->lookahead == '`') {
                advance(lexer);
                if (lexer->lookahead == '`') {
                    return has_content;
                }
            }
            has_content = true;
        } else {
            advance(lexer);
            has_content = true;
        }
    }
    return false;
}

bool tree_sitter_cfscript_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[CFML_TEMPLATE_CONTENT]) {
        return scan_cfml_template_content(lexer);
    }

    if (valid_symbols[TEMPLATE_CHARS]) {
        if (valid_symbols[AUTOMATIC_SEMICOLON]) {
            return false;
        }
        return scan_template_chars(lexer);
    }

    // if (valid_symbols[JSX_TEXT] && scan_jsx_text(lexer)) {
    //     return true;
    // }

    if (valid_symbols[QUERY_TEXT] && scan_query_text(lexer)) {
        return true;
    }

    if (valid_symbols[AUTOMATIC_SEMICOLON] && !valid_symbols[TAG_LINEFEED]) {
        bool scanned_comment = false;
        bool ret = scan_automatic_semicolon(lexer, !valid_symbols[LOGICAL_OR], &scanned_comment);
        if (!ret && !scanned_comment && valid_symbols[TERNARY_QMARK] && lexer->lookahead == '?') {
            return scan_ternary_qmark(lexer);
        }
        return ret;
    }

    if (valid_symbols[TERNARY_QMARK] || valid_symbols[ELVIS_OPERATOR]) {
        return scan_ternary_qmark(lexer);
    }

    /*if (valid_symbols[HTML_COMMENT] && !valid_symbols[LOGICAL_OR] &&
        !valid_symbols[REGEX_PATTERN]) {
        return scan_html_comment(lexer);
    }*/

    return false;
}