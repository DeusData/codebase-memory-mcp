#include "tree_sitter/parser.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <wctype.h>

// Custom punctuation check for WASM compatibility
// ispunct() is not in tree-sitter's WASM allowed functions list
// https://github.com/tree-sitter/tree-sitter/blob/master/lib/src/wasm/stdlib-symbols.txt
static inline bool is_punct_char(const uint32_t chr)
{
    if (chr >= 0x80) {
        return false;
    }

    return (chr >= 33 && chr <= 47) || // !"#$%&'()*+,-./
           (chr >= 58 && chr <= 64) || // :;<=>?@
           (chr >= 91 && chr <= 96) || // [\]^_`
           (chr >= 123 && chr <= 126); // {|}~
}

enum TokenType {
    COMMENT,
    LINE_CONTINUATION,
    COMMAND_NAME,
    COMMAND_ARGUMENT,
    SINGLE_QUOTE_STRING_START,
    SINGLE_QUOTE_STRING_END,
    DOUBLE_QUOTE_STRING_START,
    DOUBLE_QUOTE_STRING_END,
    FORMATTING_SEQUENCE,
    ESCAPE_SEQUENCE,
    STRING_CONTENT,
    ENTRY_DELIMITER,
    MULTIOUTPUT_VARIABLE_START,
    IDENTIFIER,
    CATCH_IDENTIFIER,
    TRANSPOSE,
    CTRANSPOSE,
    ERROR_SENTINEL,
};

typedef struct
{
    bool is_inside_command;
    bool line_continuation;
    bool is_shell_scape;
    char string_delimiter;
    bool generate_entry_delimiter;
} Scanner;

static const char* const keywords[] = {
    "arguments", "break",       "case",       "catch",  "classdef", "continue", "else", "elseif",
    "end",       "enumeration", "events",     "for",    "function", "global",   "if",   "methods",
    "otherwise", "parfor",      "persistent", "return", "spmd",     "switch",   "try",  "while",
};
static const size_t keywords_size = sizeof(keywords) / sizeof(keywords[0]);

static inline void advance(TSLexer* lexer)
{
    lexer->advance(lexer, false);
}

static inline void skip(TSLexer* lexer)
{
    lexer->advance(lexer, true);
}

static inline bool consume_char(char chr, TSLexer* lexer)
{
    if (lexer->lookahead != chr) {
        return false;
    }
    advance(lexer);
    return true;
}

static inline bool is_eol(const uint32_t chr)
{
    return chr == '\n' || chr == '\r' || chr == ',' || chr == ';';
}

static inline bool iswspace_matlab(const uint32_t chr)
{
    return iswspace(chr) && chr != '\n' && chr != '\r';
}

static inline bool is_identifier(const uint32_t chr, const bool start)
{
    // isalpha or isdigit is SIGSEGVing os some UTF-8 chars, like U+10C6BD
    // (0xF48C9ABD), a file with just those bytes shows the problem.
    if (chr >= 0x80) {
        return false;
    }

    const bool alpha = iswalpha(chr);
    const bool numeric = !start && iswdigit(chr);
    const bool special = chr == '_';

    return alpha || numeric || special;
}

static inline void consume_identifier(TSLexer* lexer, char* buffer)
{
    size_t size = 0;
    if (is_identifier(lexer->lookahead, true)) {
        buffer[size] = (char) lexer->lookahead;
        advance(lexer);
        while (is_identifier(lexer->lookahead, false)) {
            if (size == 255) {
                buffer[0] = 0;
                return;
            }
            buffer[++size] = (char) lexer->lookahead;
            advance(lexer);
        }
        return;
    }
    buffer[0] = 0;
}

static inline int skip_whitespaces(TSLexer* lexer)
{
    // 0b001 -> something skipped
    // 0b010 -> newline skipped
    // 0b100 -> newline was at the end of skipped sequence
    int skipped = 0;
    while (!lexer->eof(lexer) && iswspace(lexer->lookahead)) {
        skipped &= 0b011;
        if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
            skipped |= 0b111;
        } else {
            skipped |= 0b001;
        }
        skip(lexer);
    }
    return skipped;
}

static inline int consume_whitespaces(TSLexer* lexer)
{
    int skipped = 0;
    while (iswspace(lexer->lookahead)) {
        skipped &= 0b011;
        if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
            skipped |= 0b111;
        } else {
            skipped |= 0b001;
        }
        advance(lexer);
    }
    return skipped;
}

static inline void consume_whitespaces_once(TSLexer* lexer)
{
    while (!lexer->eof(lexer) && iswspace(lexer->lookahead)) {
        if (lexer->lookahead == '\n' || lexer->lookahead == '\r') {
            advance(lexer);
            break;
        }
        advance(lexer);
    }
}

void* tree_sitter_matlab_external_scanner_create()
{
    Scanner* scanner = calloc(1, sizeof(Scanner));
    return scanner;
}

void tree_sitter_matlab_external_scanner_destroy(void* payload)
{
    if (payload != NULL) {
        free(payload);
    }
}

unsigned tree_sitter_matlab_external_scanner_serialize(void* payload, char* buffer)
{
    Scanner* scanner = (Scanner*) payload;
    buffer[0] = (char) scanner->is_inside_command;
    buffer[1] = (char) scanner->line_continuation;
    buffer[2] = (char) scanner->is_shell_scape;
    buffer[3] = scanner->string_delimiter;
    buffer[4] = (char) scanner->generate_entry_delimiter;
    return 5;
}

void tree_sitter_matlab_external_scanner_deserialize(
    void* payload,
    const char* buffer,
    unsigned length)
{
    Scanner* scanner = (Scanner*) payload;
    if (length == 5) {
        scanner->is_inside_command = buffer[0];
        scanner->line_continuation = buffer[1];
        scanner->is_shell_scape = buffer[2];
        scanner->string_delimiter = buffer[3];
        scanner->generate_entry_delimiter = buffer[4];
    }
}

static inline void consume_comment_line(TSLexer* lexer)
{
    while (lexer->lookahead != '\n' && lexer->lookahead != '\r' && !lexer->eof(lexer)) {
        advance(lexer);
    }
}

// NOLINTNEXTLINE(*misc-no-recursion)
static bool scan_comment(
    Scanner* scanner,
    TSLexer* lexer,
    bool entry_delimiter,
    bool ctranspose,
    int skipped)
{
    lexer->mark_end(lexer);

    const bool percent = lexer->lookahead == '%';
    const bool line_continuation = lexer->lookahead == '.' && consume_char('.', lexer)
                                   && consume_char('.', lexer) && consume_char('.', lexer);
    const bool block = percent && consume_char('%', lexer) && consume_char('{', lexer);

    // Since we cannot look multiple chars ahead in the main function, this
    // ended up being handled here. It allows the correct detection of numbers
    // like .5 inside matrices/cells: [0 .5].
    if (entry_delimiter && !percent && !line_continuation) {
        if (iswdigit(lexer->lookahead)) {
            lexer->result_symbol = ENTRY_DELIMITER;
            return true;
        }
        if (lexer->lookahead == '\'') {
            advance(lexer);
            lexer->result_symbol = CTRANSPOSE;
            lexer->mark_end(lexer);
            return skipped == 0;
        }
        return false;
    }

    // We are inside a matrix/cell row and there is a line continuation, like this:
    // a = { 1 ...
    //       2 ...
    // }
    if (entry_delimiter && line_continuation) {
        consume_comment_line(lexer);
        consume_whitespaces(lexer);

        lexer->mark_end(lexer);
        lexer->result_symbol = LINE_CONTINUATION;

        const bool is_alpha = iswalpha(lexer->lookahead);
        const bool is_digit = iswdigit(lexer->lookahead);
        const bool is_meta = lexer->lookahead == '?' || lexer->lookahead == '@';
        const bool is_quote = lexer->lookahead == '\'' || lexer->lookahead == '"';
        const bool is_container = lexer->lookahead == '{' || lexer->lookahead == '['
                                  || lexer->lookahead == '(';

        if (lexer->lookahead == '~') {
            advance(lexer);
            scanner->generate_entry_delimiter = lexer->lookahead != '=';
        } else if (lexer->lookahead == '+' || lexer->lookahead == '-') {
            advance(lexer);
            scanner->generate_entry_delimiter = lexer->lookahead != ' ';
        } else if (lexer->lookahead == '.') {
            advance(lexer);
            scanner->generate_entry_delimiter = is_digit;
        } else if (is_alpha || is_digit || is_quote || is_container || is_meta) {
            scanner->generate_entry_delimiter = true;
        }
        return true;
    }

    if (block) {
        if (skipped & 2) {
            return false;
        }

        // If it has things on the same line, it's not a block, just a comment
        while (!lexer->eof(lexer) && iswspace_matlab(lexer->lookahead)) {
            advance(lexer);
        }
        if (!consume_char('\n', lexer) && !consume_char('\r', lexer)) {
            consume_comment_line(lexer);
            lexer->result_symbol = COMMENT;
            lexer->mark_end(lexer);
            return true;
        }

        // Otherwise, find the matching closing block
        int level = 1;
        while (!lexer->eof(lexer)) {
            consume_whitespaces(lexer);
            if (consume_char('%', lexer)) {
                if (consume_char('{', lexer) && (consume_whitespaces(lexer) & 2)) {
                    level++;
                } else if (consume_char('}', lexer)) {
                    lexer->mark_end(lexer);
                    if (consume_whitespaces(lexer) & 2) {
                        level--;
                    }
                }
                if (level == 0) {
                    break;
                }
                continue;
            }
            consume_comment_line(lexer);
            lexer->mark_end(lexer);
        }

        lexer->result_symbol = COMMENT;
        return true;
    }

    if (percent || line_continuation) {
        consume_comment_line(lexer);
        lexer->mark_end(lexer);

        if (!line_continuation) {
            lexer->result_symbol = COMMENT;
            advance(lexer);
        } else {
            lexer->result_symbol = LINE_CONTINUATION;
            consume_whitespaces_once(lexer);
            lexer->mark_end(lexer);
            return true;
        }

        // Merges consecutive comments into one token, unless they are
        // separated by a newline.
        while (!lexer->eof(lexer) && (lexer->lookahead == ' ' || lexer->lookahead == '\t')) {
            advance(lexer);
        }

        if (lexer->lookahead == '%') {
            return scan_comment(scanner, lexer, false, false, 0);
        }

        return true;
    }

    if (ctranspose && lexer->lookahead == '\'') {
        advance(lexer);
        lexer->mark_end(lexer);
        lexer->result_symbol = CTRANSPOSE;
        return true;
    }

    return false;
}

static bool scan_command(Scanner* scanner, TSLexer* lexer, const bool* valid_symbols)
{
    // Special case: shell escape
    if (lexer->lookahead == '!') {
        advance(lexer);
        while (iswspace_matlab(lexer->lookahead)) {
            advance(lexer);
        }
        while (lexer->lookahead != ' ' && lexer->lookahead != '\n' && !lexer->eof(lexer)) {
            advance(lexer);
        }
        lexer->result_symbol = COMMAND_NAME;
        lexer->mark_end(lexer);
        while (iswspace_matlab(lexer->lookahead)) {
            advance(lexer);
        }
        scanner->is_inside_command = lexer->lookahead != '\n';
        scanner->is_shell_scape = scanner->is_inside_command;
        return true;
    }

    if (!is_identifier(lexer->lookahead, true)) {
        return false;
    }

    char buffer[256] = {0};
    consume_identifier(lexer, buffer);
    lexer->mark_end(lexer);
    const char* allowed_commands[] = {"methods", "arguments", "enumeration", "events"};
    if (buffer[0] != 0) {
        if (lexer->lookahead == '.') {
            // Since it is not followed by a space, it cannot be a command.
            if ((strcmp("get", buffer) == 0 || strcmp("set", buffer) == 0)) {
                return false;
            }
            // so it is ok to consume to identify a line continuation
            // NOLINTNEXTLINE(*misc-redundant-expression)
            if (consume_char('.', lexer) && consume_char('.', lexer) && consume_char('.', lexer)) {
                // If it is a keyword, yield to the internal scanner
                for (size_t i = 0; i < keywords_size; i++) {
                    if (strcmp(keywords[i], buffer) == 0) {
                        if (strcmp("enumeration", buffer) == 0) {
                            goto check_enumeration;
                        }
                        return false;
                    }
                }
            }
            lexer->result_symbol = IDENTIFIER;
            return true;
        }
        // The following keywords are allowed as commands if they get 1 argument
        for (unsigned i = 0; i < sizeof(allowed_commands) / sizeof(allowed_commands[0]); i++) {
            if (strcmp(allowed_commands[i], buffer) == 0) {
                goto check_command_for_argument;
            }
        }
        for (unsigned i = 0; i < keywords_size; i++) {
            if (strcmp(keywords[i], buffer) == 0) {
                return false;
            }
        }
    }
    goto skip_command_check;

check_enumeration: {
    const int skipped = consume_whitespaces(lexer);
    if (skipped & 2) {
        // enumeration can be a function
        if (lexer->lookahead == '(') {
            lexer->result_symbol = IDENTIFIER;
            return true;
        }
    }
    return false;
}

check_command_for_argument:
    // If this is a keyword-command, check if it has an argument.
    // If it has no arguments, this is a keyword, not a command.
    lexer->result_symbol = COMMAND_NAME;
    while (!lexer->eof(lexer) && iswspace_matlab(lexer->lookahead)) {
        advance(lexer);
    }
    if (is_identifier(lexer->lookahead, true)) {
        scanner->is_inside_command = true;
        return true;
    }
    return false;

skip_command_check:
    // First case: found an end-of-line already, so this is a command for sure.
    // example:
    // pwd
    // pwd;
    // pwd,
    if (is_eol(lexer->lookahead)) {
        lexer->result_symbol = valid_symbols[CATCH_IDENTIFIER] ? CATCH_IDENTIFIER : COMMAND_NAME;
        return true;
    }

    // If it's not followed by a space, it may be something else, like A' for
    // example. Or A+2.
    if (lexer->lookahead != ' ') {
        lexer->result_symbol = IDENTIFIER;
        return true;
    }

    // If followed by a line continuation, look after it
    const int skipped = consume_whitespaces(lexer);
    if (skipped & 2) {
        // `catch e `
        if (valid_symbols[CATCH_IDENTIFIER]) {
            lexer->result_symbol = CATCH_IDENTIFIER;
            return true;
        }
        // Command followed by spaces then newline
        scanner->is_inside_command = false;
        lexer->result_symbol = COMMAND_NAME;
        return true;
    }
    if (lexer->lookahead == '.' && consume_char('.', lexer) && consume_char('.', lexer)
        && consume_char('.', lexer)) {
        lexer->result_symbol = IDENTIFIER;
        return true;
    }

    // If it is followed by a space, it doesn't mean it's a command yet.
    // It could be A + 2 or A = 2. Let's check what is the first char after
    // all whitespaces. We mark it already as this is the right place, and we
    // only need to make sure this is a command and not something else from
    // this point on.
    lexer->result_symbol = COMMAND_NAME;
    while (!lexer->eof(lexer) && iswspace_matlab(lexer->lookahead)) {
        advance(lexer);
    }

    // Check for end-of-line again, since it may be that the user just put a
    // space at the end, like `pwd ;`
    if (is_eol(lexer->lookahead) || lexer->lookahead == '%') {
        if (valid_symbols[CATCH_IDENTIFIER] && (skipped & 4) == 0) {
            lexer->result_symbol = CATCH_IDENTIFIER;
            return true;
        }
        scanner->is_inside_command = true;
        return true;
    }

    // The first char of the first argument cannot be /=()/
    if (lexer->lookahead == '=' || lexer->lookahead == '(' || lexer->lookahead == ')') {
        lexer->result_symbol = IDENTIFIER;
        return true;
    }

    // If it is a single quote, it is a command.
    if (lexer->lookahead == '\'') {
        scanner->is_inside_command = true;
        return true;
    }

    // If it is an identifier char, then it's a command
    if (is_identifier(lexer->lookahead, false)) {
        scanner->is_inside_command = true;
        return true;
    }

    // If it is a char greater than 0xC0, then assume it's a valid UTF-8
    // char, and that this is a command.
    if (lexer->lookahead >= 0xC0) {
        scanner->is_inside_command = true;
        return true;
    }

    // Let's now consider punctuation marks.
    if (is_punct_char(lexer->lookahead)) {
        // In this case, we advance and look at what comes next too.
        const uint32_t first = lexer->lookahead;
        advance(lexer);
        const uint32_t second = lexer->lookahead;

        // If it's the end-of-line, then it's a command.
        if (is_eol(second)) {
            scanner->is_inside_command = true;
            return true;
        }

        if (iswspace_matlab(second)) {
            // If it is a space, then it depends on what we have, since
            // `disp + ;` is a valid command but `disp + 2;` isn't.
            const char operators[] = {
                '!',
                '&',
                '*',
                '+',
                '-',
                '/',
                '<',
                '>',
                '@',
                '\\',
                '^',
                '|',
            };
            bool is_invalid = false;
            for (size_t i = 0; i < sizeof(operators); i++) {
                if (first == (uint32_t) operators[i]) {
                    is_invalid = true;
                    break;
                }
            }
            // If it is an operator, this can only be a command if there
            // are no further arguments.
            if (is_invalid) {
                advance(lexer);
                while (iswspace_matlab(lexer->lookahead)) {
                    advance(lexer);
                }
                scanner->is_inside_command = is_eol(lexer->lookahead);
                lexer->result_symbol = scanner->is_inside_command ? COMMAND_NAME : IDENTIFIER;
                return true;
            }

            // If it's not an operator, then this is a command.
            scanner->is_inside_command = true;
            return true;
        }

        // Now we check for the rest of the operators.
        // Since they have 2 digits, it matters if the next is a space.
        advance(lexer);

        if (lexer->lookahead != ' ') {
            scanner->is_inside_command = true;
            return true;
        }

        const char operators[][2] = {
            {'&', '&'},
            {'|', '|'},
            {'=', '='},
            {'~', '='},
            {'<', '='},
            {'>', '='},
            {'.', '+'},
            {'.', '-'},
            {'.', '*'},
            {'.', '/'},
            {'.', '\\'},
            {'.', '^'},
        };

        for (int i = 0; i < 12; i++) {
            if ((uint32_t) operators[i][0] == first && (uint32_t) operators[i][1] == second) {
                lexer->result_symbol = IDENTIFIER;
                return true;
            }
        }

        scanner->is_inside_command = true;
        return true;
    }

    return false;
}

static bool scan_command_argument(Scanner* scanner, TSLexer* lexer)
{
    // If this is a shell escape command, we just break arguments in spaces
    // since we don't know what shell it is.
    if (scanner->is_shell_scape) {
        if (lexer->eof(lexer)) {
            return false;
        }

        while (lexer->lookahead != ' ' && lexer->lookahead != '\n' && !lexer->eof(lexer)) {
            advance(lexer);
        }
        lexer->result_symbol = COMMAND_ARGUMENT;
        lexer->mark_end(lexer);
        while (iswspace_matlab(lexer->lookahead)) {
            advance(lexer);
        }
        if (lexer->lookahead == '\n') {
            scanner->is_inside_command = false;
            scanner->is_shell_scape = false;
        }
        return true;
    }

    // Avoids infinite loop when the argument is right before the eof.
    if (lexer->eof(lexer)) {
        return false;
    }

    bool quote = false;
    int32_t parens = 0;
    bool consumed = false;

    while (!lexer->eof(lexer)) {
        // No matter what, found new line
        const bool cond1 = lexer->lookahead == '\n' || lexer->lookahead == '\r';
        // No quotes, no parens, found $._end_of_line or space
        const bool cond2 = !quote && parens == 0
                           && (is_eol(lexer->lookahead) || iswspace_matlab(lexer->lookahead));
        // Inside parens, no quotes, found ;
        const bool cond3 = !quote && parens != 0 && lexer->lookahead == ';';
        if (cond1 || cond2 || cond3) {
            lexer->result_symbol = COMMAND_ARGUMENT;
            lexer->mark_end(lexer);

            while (iswspace_matlab(lexer->lookahead)) {
                advance(lexer);
            }

            if (is_eol(lexer->lookahead) || cond1) {
                scanner->line_continuation = false;
                scanner->is_inside_command = false;
            }

            return true;
        }

        // Line comment, finish.
        if ((!quote || (quote && parens != 0)) && lexer->lookahead == '%') {
            scanner->is_inside_command = false;
            if (consumed) {
                lexer->result_symbol = COMMAND_ARGUMENT;
                lexer->mark_end(lexer);
                return true;
            }
            return scan_comment(scanner, lexer, false, false, 0);
        }

        // Line continuation
        if ((!quote || (quote && parens != 0)) && lexer->lookahead == '.') {
            lexer->result_symbol = COMMAND_ARGUMENT;
            lexer->mark_end(lexer);
            advance(lexer);
            if (lexer->lookahead == '.') {
                advance(lexer);
                if (lexer->lookahead == '.') {
                    if (consumed) {
                        scanner->line_continuation = true;
                    } else {
                        consume_comment_line(lexer);
                        lexer->result_symbol = LINE_CONTINUATION;
                        lexer->mark_end(lexer);
                    }
                    return true;
                }
                consumed = true;
                continue;
            }
            consumed = true;
            continue;
        }

        if ((lexer->lookahead == '(' || lexer->lookahead == '[' || lexer->lookahead == '{')
            && (!quote || (quote && parens != 0))) {
            parens++;
        }

        if ((lexer->lookahead == ')' || lexer->lookahead == ']' || lexer->lookahead == '}')
            && (!quote || (quote && parens != 0))) {
            parens--;
        }

        if (lexer->lookahead == '\'') {
            quote = !quote;
        }

        advance(lexer);
        consumed = true;
    }

    // Mark as argument so the scanner doesnt get called again in an infinite
    // loop.
    if (lexer->eof(lexer)) {
        lexer->result_symbol = COMMAND_ARGUMENT;
        lexer->mark_end(lexer);
        return true;
    }

    return false;
}

static bool scan_string_open(Scanner* scanner, TSLexer* lexer)
{
    switch (lexer->lookahead) {
    case '"':
        scanner->string_delimiter = '"';
        advance(lexer);
        lexer->result_symbol = DOUBLE_QUOTE_STRING_START;
        lexer->mark_end(lexer);
        return true;
    case '\'':
        scanner->string_delimiter = '\'';
        advance(lexer);
        lexer->result_symbol = SINGLE_QUOTE_STRING_START;
        lexer->mark_end(lexer);
        // A single quote string has to be ended in the same line.
        while (!lexer->eof(lexer) && lexer->lookahead != '\n') {
            if (lexer->lookahead == '\'') {
                return true;
            }
            advance(lexer);
        }
        return false;
    default:
        return false;
    }
}

static bool scan_string_close(Scanner* scanner, TSLexer* lexer)
{
    if (lexer->lookahead == scanner->string_delimiter) {
        advance(lexer);
        if (lexer->lookahead == scanner->string_delimiter) {
            advance(lexer);
            lexer->result_symbol = STRING_CONTENT;
            goto content;
        }
        lexer->result_symbol = scanner->string_delimiter == '"' ? DOUBLE_QUOTE_STRING_END
                                                                : SINGLE_QUOTE_STRING_END;
        lexer->mark_end(lexer);
        scanner->string_delimiter = 0;
        return true;
    }

    // This means this string is not properly terminated.
    if (lexer->lookahead == '\n' || lexer->lookahead == '\r' || lexer->eof(lexer)) {
        scanner->string_delimiter = 0;
        return false;
    }

    if (lexer->lookahead == '%') {
        advance(lexer);

        if (lexer->lookahead == '%') {
            advance(lexer);
            lexer->result_symbol = FORMATTING_SEQUENCE;
            lexer->mark_end(lexer);
            return true;
        }

        const char* valid_tokens = "1234567890.-+ #btcdeEfgGosuxX";
        const char* end_tokens = "cdeEfgGosuxX";
        while (!lexer->eof(lexer) && lexer->lookahead != '\n' && lexer->lookahead != '\r') {
            bool is_valid = false;
            for (size_t i = 0; i < strlen(valid_tokens); i++) {
                if ((int32_t) valid_tokens[i] == lexer->lookahead) {
                    is_valid = true;
                    break;
                }
            }

            if (!is_valid) {
                lexer->result_symbol = STRING_CONTENT;
                goto content;
            }

            for (int i = 0; i < 12; i++) {
                if (end_tokens[i] == lexer->lookahead) {
                    advance(lexer);
                    lexer->result_symbol = FORMATTING_SEQUENCE;
                    lexer->mark_end(lexer);
                    return true;
                }
            }

            advance(lexer);
        }

        scanner->string_delimiter = 0;
        return false;
    }

    if (lexer->lookahead == '\\') {
        advance(lexer);

        if (lexer->lookahead == 'x') {
            advance(lexer);
            while (!lexer->eof(lexer)) {
                const char* hexa_chars = "1234567890abcdefABCDEF";
                bool is_valid = false;
                for (int i = 0; i < 22; i++) {
                    if (hexa_chars[i] == lexer->lookahead) {
                        is_valid = true;
                        break;
                    }
                }

                if (!is_valid) {
                    lexer->result_symbol = ESCAPE_SEQUENCE;
                    lexer->mark_end(lexer);
                    return true;
                }

                advance(lexer);
            }
        }

        if (lexer->lookahead >= '0' && lexer->lookahead <= '7') {
            while (lexer->lookahead >= '0' && lexer->lookahead <= '7' && !lexer->eof(lexer)) {
                advance(lexer);
            }

            lexer->result_symbol = ESCAPE_SEQUENCE;
            lexer->mark_end(lexer);
            return true;
        }

        const char* escapes = "abfnrtv\\";
        bool is_valid = false;
        for (int i = 0; i < 8; i++) {
            if (escapes[i] == lexer->lookahead) {
                is_valid = true;
                break;
            }
        }

        if (is_valid) {
            advance(lexer);
            lexer->result_symbol = ESCAPE_SEQUENCE;
            lexer->mark_end(lexer);
            return true;
        }
    }

content:
    while (lexer->lookahead != '\n' && lexer->lookahead != '\r' && !lexer->eof(lexer)) {
        // In MATLAB '' and "" are valid inside their own kind: 'It''s ok' "He said ""it's ok"""
        if (lexer->lookahead == scanner->string_delimiter) {
            lexer->result_symbol = STRING_CONTENT;
            lexer->mark_end(lexer);
            advance(lexer);
            if (lexer->lookahead != scanner->string_delimiter) {
                return true;
            }
            advance(lexer);
            continue;
        }

        // The scanner will be called again, and this time we will match in the if
        // before this while.
        if (lexer->lookahead == '%' || lexer->lookahead == '\\') {
            lexer->result_symbol = STRING_CONTENT;
            lexer->mark_end(lexer);
            advance(lexer);
            if (lexer->lookahead == scanner->string_delimiter || iswspace_matlab(lexer->lookahead)) {
                goto content;
            }
            return true;
        }

        advance(lexer);
    }

    // Mark end of content here and end of string on next call. This is an
    // unterminated string and it's better to wrongly finish it here, otherwise
    // the error will appear god knows how many lines after this and it will be
    // hard for the user to understand what went wrong.
    if (lexer->lookahead == '\n' || lexer->lookahead == '\r' || lexer->eof(lexer)) {
        lexer->result_symbol = STRING_CONTENT;
        lexer->mark_end(lexer);
        return true;
    }

    scanner->string_delimiter = 0;
    return false;
}

static inline bool scan_multioutput_var_start(TSLexer* lexer)
{
    advance(lexer);
    lexer->result_symbol = MULTIOUTPUT_VARIABLE_START;
    lexer->mark_end(lexer);

    // We can have arrays inside function calls inside the multi-output variable, so we have to keep
    // track.
    unsigned sb_count = 0;

    while (!lexer->eof(lexer)) {
        // NOLINTNEXTLINE(*misc-redundant-expression)
        if (consume_char('.', lexer) && consume_char('.', lexer) && consume_char('.', lexer)) {
            consume_comment_line(lexer);
            advance(lexer);
        }

        if (lexer->lookahead == '[') {
            sb_count++;
            advance(lexer);
        }

        if (lexer->lookahead != ']') {
            advance(lexer);
        } else if (sb_count > 0) {
            sb_count--;
            advance(lexer);
        } else {
            break;
        }
    }

    if (lexer->lookahead != ']') {
        return false;
    }

    advance(lexer);

    while (!lexer->eof(lexer)) {
        // NOLINTNEXTLINE(*misc-redundant-expression)
        if (consume_char('.', lexer) && consume_char('.', lexer) && consume_char('.', lexer)) {
            consume_comment_line(lexer);
            advance(lexer);
        } else if (iswspace_matlab(lexer->lookahead)) {
            advance(lexer);
        } else {
            break;
        }
    }

    if (lexer->lookahead == '=') {
        advance(lexer);
        if (lexer->lookahead != '=') {
            return true;
        }
    }

    return false;
}

static bool scan_identifier(TSLexer* lexer);
static bool scan_entry_delimiter(TSLexer* lexer, int skipped)
{
    lexer->mark_end(lexer);
    lexer->result_symbol = ENTRY_DELIMITER;

    if (skipped & 2) {
        return false;
    }

    if (lexer->lookahead == ',') {
        advance(lexer);
        lexer->mark_end(lexer);
        lexer->result_symbol = ENTRY_DELIMITER;
        return true;
    }

    if (lexer->lookahead == '.') {
        advance(lexer);
        advance(lexer);
        return iswdigit(lexer->lookahead);
    }

    if (lexer->lookahead == '{' || lexer->lookahead == '(' || lexer->lookahead == '\'') {
        return skipped != 0;
    }

    if (lexer->lookahead == '[') {
        return true;
    }

    // These chars mean we cannot end the cell here, as the expression will
    // surely continue OR we need to just leave the char there and the internal
    // parser will do the rest.
    const char no_end[] = {']', '}', '&', '|', '=', '<', '>', '*', '/', '\\', '^', ';', ':'};
    for (size_t i = 0; i < sizeof(no_end); i++) {
        if ((int32_t) no_end[i] == lexer->lookahead) {
            return false;
        }
    }

    if (lexer->lookahead == '~') {
        advance(lexer);
        return lexer->lookahead != '=';
    }

    const char maybe_end[] = {'+', '-'};
    for (size_t i = 0; i < sizeof(maybe_end); i++) {
        if ((int32_t) maybe_end[i] == lexer->lookahead) {
            advance(lexer);
            if (lexer->lookahead == ' ') {
                return false;
            }
            return skipped != 0;
        }
    }

    if (skipped != 0) {
        return true;
    }

    if (is_identifier(lexer->lookahead, true)) {
        return scan_identifier(lexer);
    }

    return false;
}

static bool scan_identifier(TSLexer* lexer)
{
    char buffer[256] = {0};
    consume_identifier(lexer, buffer);
    if (buffer[0] != 0) {
        if (lexer->lookahead == '.') {
            if ((strcmp("get", buffer) == 0 || strcmp("set", buffer) == 0)) {
                return false;
            }
            lexer->result_symbol = IDENTIFIER;
            lexer->mark_end(lexer);
            return true;
        }
        for (size_t i = 0; i < keywords_size; i++) {
            if (strcmp(keywords[i], buffer) == 0) {
                return false;
            }
        }
        lexer->result_symbol = IDENTIFIER;
        lexer->mark_end(lexer);
        return true;
    }
    return false;
}

static bool scan_transpose(TSLexer* lexer)
{
    if (lexer->lookahead == '\'') {
        advance(lexer);
        lexer->mark_end(lexer);
        lexer->result_symbol = TRANSPOSE;
        return true;
    }
    if (lexer->lookahead == '.' && consume_char('\'', lexer)) {
        advance(lexer);
        lexer->mark_end(lexer);
        lexer->result_symbol = CTRANSPOSE;
        return true;
    }
    return false;
}

bool tree_sitter_matlab_external_scanner_scan(void* payload, TSLexer* lexer, const bool* valid_symbols)
{
    Scanner* scanner = (Scanner*) payload;

    if (scanner->generate_entry_delimiter) {
        scanner->generate_entry_delimiter = false;
        lexer->mark_end(lexer);
        lexer->result_symbol = ENTRY_DELIMITER;
        return true;
    }

    if (scanner->string_delimiter == 0) {
        int skipped = skip_whitespaces(lexer);

        if ((scanner->line_continuation || !scanner->is_inside_command) && valid_symbols[COMMENT]
            && (lexer->lookahead == '%' || ((skipped & 2) == 0 && lexer->lookahead == '.'))) {
            return scan_comment(
                scanner, lexer, valid_symbols[ENTRY_DELIMITER], valid_symbols[CTRANSPOSE], skipped);
        }

        if (!scanner->is_inside_command) {
            if (skipped == 0 && valid_symbols[TRANSPOSE]) {
                if (scan_transpose(lexer)) {
                    return true;
                }
            }

            if ((valid_symbols[SINGLE_QUOTE_STRING_START] && lexer->lookahead == '\'')
                || (valid_symbols[DOUBLE_QUOTE_STRING_START] && lexer->lookahead == '"')) {
                return scan_string_open(scanner, lexer);
            }

            if (!scanner->line_continuation) {
                if (valid_symbols[MULTIOUTPUT_VARIABLE_START] && lexer->lookahead == '[') {
                    return scan_multioutput_var_start(lexer);
                }

                if (valid_symbols[ENTRY_DELIMITER]) {
                    return scan_entry_delimiter(lexer, skipped);
                }
            }

            if (valid_symbols[COMMAND_NAME]) {
                scanner->is_inside_command = false;
                scanner->is_shell_scape = false;
                return scan_command(scanner, lexer, valid_symbols);
            }

            if (valid_symbols[IDENTIFIER] && (skipped & 2) == 0) {
                scanner->is_inside_command = false;
                scanner->is_shell_scape = false;
                return scan_identifier(lexer);
            }
        } else {
            if (valid_symbols[COMMAND_ARGUMENT]) {
                return scan_command_argument(scanner, lexer);
            }
        }
    } else {
        if (valid_symbols[DOUBLE_QUOTE_STRING_END] || valid_symbols[SINGLE_QUOTE_STRING_END]
            || valid_symbols[FORMATTING_SEQUENCE]) {
            return scan_string_close(scanner, lexer);
        }
    }

    return false;
}