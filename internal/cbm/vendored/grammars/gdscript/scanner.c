#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "tree_sitter/parser.h"

#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define VEC_RESIZE(vec, _cap)                                                  \
    void *tmp = realloc((vec)->data, (_cap) * sizeof((vec)->data[0]));         \
    assert(tmp != NULL);                                                       \
    (vec)->data = tmp;                                                         \
    (vec)->cap = (_cap);

#define VEC_GROW(vec, _cap)                                                    \
    if ((vec)->cap < (_cap)) {                                                 \
        VEC_RESIZE((vec), (_cap));                                             \
    }

#define VEC_PUSH(vec, el)                                                      \
    if ((vec)->cap == (vec)->len) {                                            \
        VEC_RESIZE((vec), MAX(16, (vec)->len * 2));                            \
    }                                                                          \
    (vec)->data[(vec)->len++] = (el);

#define VEC_POP(vec) (vec)->len--;

#define VEC_NEW                                                                \
    { .len = 0, .cap = 0, .data = NULL }

#define VEC_BACK(vec) ((vec)->data[(vec)->len - 1])

#define VEC_FREE(vec)                                                          \
    {                                                                          \
        if ((vec)->data != NULL)                                               \
            free((vec)->data);                                                 \
    }

#define VEC_CLEAR(vec)                                                         \
    { (vec)->len = 0; }

enum TokenType {
    NEWLINE,
    INDENT,
    DEDENT,
    STRING_START,
    STRING_CONTENT,
    STRING_END,
    STRING_NAME_START,
    NODE_PATH_START,
    COMMENT,
    CLOSE_PAREN,
    CLOSE_BRACKET,
    CLOSE_BRACE,
    COMMA,
    /* COLON, // See grammar.js externals */
    BODY_END,
    REGION_START,
    REGION_END,
};

typedef enum {
    SingleQuote = 1 << 0,
    DoubleQuote = 1 << 1,
    Triple = 1 << 2,
    Raw = 1 << 3,
    Name = 1 << 4,
    NodePath = 1 << 5,
} Flags;

typedef struct {
    char flags;
} Delimiter;

static inline Delimiter new_delimiter() { return (Delimiter){0}; }

static inline bool is_triple(Delimiter *delimiter) {
    return delimiter->flags & Triple;
}

static inline bool is_raw(Delimiter *delimiter) {
    return delimiter->flags & Raw;
}

static inline bool is_name(Delimiter *delimiter) {
    return delimiter->flags & Name;
}

static inline bool is_node_path(Delimiter *delimiter) {
    return delimiter->flags & NodePath;
}

static inline int32_t end_character(Delimiter *delimiter) {
    if (delimiter->flags & SingleQuote) {
        return '\'';
    }
    if (delimiter->flags & DoubleQuote) {
        return '"';
    }
    return 0;
}

static inline void set_triple(Delimiter *delimiter) {
    delimiter->flags |= Triple;
}

static inline void set_raw(Delimiter *delimiter) {
    delimiter->flags |= Raw;
}

static inline void set_name(Delimiter *delimiter) {
    delimiter->flags |= Name;
}

static inline void set_node_path(Delimiter *delimiter) {
    delimiter->flags |= NodePath;
}

static inline void set_end_character(Delimiter *delimiter, int32_t character) {
    switch (character) {
        case '\'':
            delimiter->flags |= SingleQuote;
            break;
        case '"':
            delimiter->flags |= DoubleQuote;
            break;
        default:
            assert(false);
    }
}

static inline const char *delimiter_string(Delimiter *delimiter) {
    if (delimiter->flags & SingleQuote) {
        return "\'";
    }
    if (delimiter->flags & DoubleQuote) {
        return "\"";
    }
    return "";
}

typedef struct {
    uint32_t len;
    uint32_t cap;
    uint16_t *data;
} indent_vec;

typedef struct {
    uint32_t len;
    uint32_t cap;
    Delimiter *data;
} delimiter_vec;

typedef struct {
    indent_vec *indents;
    delimiter_vec *delimiters;
    bool has_inline_comment;
} Scanner;

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

// Helper function to check for a string from current lexer position
// Used for matching the "region" and "endregion" keywords.
static bool look_ahead_string(TSLexer *lexer, const char *str) {
    for (int i = 0; str[i] != '\0'; i++) {
        char c = str[i];
        if (lexer->lookahead != c) {
            return false;
        }
        advance(lexer);
    }
    return true;
}

static inline void handle_quote(TSLexer *lexer, Delimiter *delimiter, char quote) {
    set_end_character(delimiter, quote);
    advance(lexer);
    lexer->mark_end(lexer);

    if (lexer->lookahead == quote) {
        advance(lexer);
        if (lexer->lookahead == quote) {
            advance(lexer);
            lexer->mark_end(lexer);
            set_triple(delimiter);
        }
    }
}

bool tree_sitter_gdscript_external_scanner_scan(void *payload, TSLexer *lexer,
                                                const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;

    bool error_recovery_mode =
        valid_symbols[STRING_CONTENT] && valid_symbols[INDENT];
    bool within_brackets = valid_symbols[CLOSE_BRACE] ||
                           valid_symbols[CLOSE_PAREN] ||
                           valid_symbols[CLOSE_BRACKET];

    if (valid_symbols[STRING_CONTENT] && scanner->delimiters->len > 0 &&
        !error_recovery_mode) {
        Delimiter delimiter = VEC_BACK(scanner->delimiters);
        int32_t end_char = end_character(&delimiter);
        bool has_content = false;
        while (lexer->lookahead) {
            if (lexer->lookahead == '\\') {
                if (is_raw(&delimiter)) {
                    // Step over the backslash.
                    lexer->advance(lexer, false);
                    // Step over any escaped quotes.
                    if (lexer->lookahead == end_character(&delimiter) ||
                        lexer->lookahead == '\\') {
                        lexer->advance(lexer, false);
                    }
                    continue;
                } else {
                    lexer->mark_end(lexer);
                    lexer->result_symbol = STRING_CONTENT;
                    return has_content;
                }
            } else if (lexer->lookahead == end_char) {
                if (is_triple(&delimiter)) {
                    lexer->mark_end(lexer);
                    lexer->advance(lexer, false);
                    if (lexer->lookahead == end_char) {
                        lexer->advance(lexer, false);
                        if (lexer->lookahead == end_char) {
                            if (has_content) {
                                lexer->result_symbol = STRING_CONTENT;
                            } else {
                                lexer->advance(lexer, false);
                                lexer->mark_end(lexer);
                                VEC_POP(scanner->delimiters);
                                lexer->result_symbol = STRING_END;
                            }
                            return true;
                        }
                        lexer->mark_end(lexer);
                        lexer->result_symbol = STRING_CONTENT;
                        return true;
                    }
                    lexer->mark_end(lexer);
                    lexer->result_symbol = STRING_CONTENT;
                    return true;
                }
                if (has_content) {
                    lexer->result_symbol = STRING_CONTENT;
                } else {
                    lexer->advance(lexer, false);
                    VEC_POP(scanner->delimiters);
                    lexer->result_symbol = STRING_END;
                }
                lexer->mark_end(lexer);
                return true;

            } else if (lexer->lookahead == '\n' && has_content &&
                       !is_triple(&delimiter)) {
                return false;
            }
            advance(lexer);
            has_content = true;
        }
    }

    lexer->mark_end(lexer);

    bool found_end_of_line = false;
    uint32_t indent_length = 0;
    int32_t first_comment_indent_length = -1;
    for (;;) {
        if (lexer->lookahead == '\n') {
            found_end_of_line = true;
            indent_length = 0;
            scanner->has_inline_comment = false;
            skip(lexer);
        } else if (lexer->lookahead == ' ') {
            indent_length++;
            skip(lexer);
        } else if (lexer->lookahead == '\r' || lexer->lookahead == '\f') {
            indent_length = 0;
            skip(lexer);
        } else if (lexer->lookahead == '\t') {
            indent_length += 8;
            skip(lexer);
        } else if (lexer->lookahead == '#') {
            uint32_t comment_column = lexer->get_column(lexer);
            // We check if the comment marker comes right after the indent to
            // determine if it's located at the start of the line. If so it's a
            // standalone comment. Otherwise, it's inline with code.
            bool is_comment_at_line_start = (comment_column == indent_length);
            bool is_region_marker = false;

            // Check for #region and #endregion in priority before handling the token as a comment.
            if (valid_symbols[REGION_START] || valid_symbols[REGION_END]) {
                // We first walk over the # character
                advance(lexer);

                // We check if the next characters might be the start or end of a region keyword to filter out
                // comments that are not region markers before making more checks.
                bool is_region = (lexer->lookahead == 'r');
                bool is_endregion = (lexer->lookahead == 'e');

                // Region delimiters are always on their own line. They have an
                // optional label after the keyword that is ignored by the
                // GDScript parser (so the opening and closing labels don't have
                // to match).
                if (valid_symbols[REGION_START] && is_region && look_ahead_string(lexer, "region")) {
                    lexer->mark_end(lexer);
                    lexer->result_symbol = REGION_START;
                    return true;
                } else if (valid_symbols[REGION_END] && is_endregion && look_ahead_string(lexer, "endregion")) {
                    // We want to capture the opening label if it is present in
                    // case someone wants to use it for text editor outline for
                    // example, but ignore the closing label. Here we consume
                    // the entire line because of that.
                    while (lexer->lookahead && lexer->lookahead != '\n' && lexer->lookahead != '\r' && !lexer->eof(lexer)) {
                        advance(lexer);
                    }
                    lexer->mark_end(lexer);
                    lexer->result_symbol = REGION_END;
                    return true;
                }
            }

            // We were not looking at a region or endregion token, handle as regular comment
            if (first_comment_indent_length == -1 && is_comment_at_line_start) {
                first_comment_indent_length = (int32_t)indent_length;
            } else if (!is_comment_at_line_start) {
                scanner->has_inline_comment = true;
            }
            while (lexer->lookahead && lexer->lookahead != '\n') {
                skip(lexer);
            }
            skip(lexer);
            indent_length = 0;
        } else if (lexer->lookahead == '\\') {
            skip(lexer);
            if (lexer->lookahead == '\r') {
                skip(lexer);
            }
            if (lexer->lookahead == '\n' || lexer->eof(lexer)) {
                skip(lexer);
            } else {
                return false;
            }
        } else if (lexer->eof(lexer)) {
            indent_length = 0;
            found_end_of_line = true;
            break;
        } else {
            break;
        }
    }

    if (found_end_of_line) {
        if (scanner->indents->len > 0) {
            uint16_t current_indent_length = VEC_BACK(scanner->indents);

            if (valid_symbols[INDENT] &&
                indent_length > current_indent_length) {
                VEC_PUSH(scanner->indents, indent_length);
                lexer->result_symbol = INDENT;
                return true;
            }

            // Check if the lexer is expecting a DEDENT token, but we're not
            // within brackets or expecting a string.
            bool dedent_explicitly_valid = valid_symbols[DEDENT];
            bool not_expecting_strings_or_in_brackets = !valid_symbols[NEWLINE] &&
                                     !valid_symbols[STRING_START] &&
                                     !valid_symbols[STRING_NAME_START] &&
                                     !valid_symbols[NODE_PATH_START] &&
                                     !within_brackets;
            bool should_generate_dedent = dedent_explicitly_valid || not_expecting_strings_or_in_brackets;
            bool indentation_has_decreased = indent_length < current_indent_length;

            // Wait to create a dedent token until we've consumed any comments
            // whose indentation matches the current block. Also delay dedent if
            // we have inline comments to allow them to be associated with the
            // current scope.
            bool comments_allow_dedent = first_comment_indent_length < (int32_t)current_indent_length &&
                                       !scanner->has_inline_comment;

            if (should_generate_dedent && indentation_has_decreased && comments_allow_dedent) {
                VEC_POP(scanner->indents);
                lexer->result_symbol = DEDENT;
                return true;
            }
        }

        if (valid_symbols[NEWLINE] && !error_recovery_mode) {
            lexer->result_symbol = NEWLINE;
            return true;
        }
    }

    // This if statement can be placed before the above if statement that
    // handles newlines. However, it feels safer to give indentation and
    // newlines higher precedence.
    if (
        // Guard against BODY_END tokens overriding valid tokens.
        !valid_symbols[COMMA] &&
        /* !valid_symbols[COLON] && */
        !valid_symbols[CLOSE_PAREN] && !valid_symbols[CLOSE_BRACE] &&
        !valid_symbols[CLOSE_BRACKET] &&

        // Body ends occur in error recovery mode since the grammar does not
        // (cannot?) specify that a body can end with the below characters
        // without consuming them itself.
        (error_recovery_mode || valid_symbols[BODY_END])) {
        if (lexer->lookahead == ',' || // separator
            lexer->lookahead == ')' || // args, params, paren expr
            lexer->lookahead == '}' || // dictionary (may not be needed)
            lexer->lookahead == ']'    // array
            /* lexer->lookahead == ':'     // key-value pairs (breaks if
               elses) */
        ) {
            // BODY_END tokens can take the place of a dedent. Therefore, we
            // should pop the stack when DEDENT is valid.
            if (valid_symbols[DEDENT] && scanner->indents->len > 0) {
                VEC_POP(scanner->indents);
            }
            lexer->result_symbol = BODY_END;
            return true;
        }
    }

    if (first_comment_indent_length == -1 &&
            (valid_symbols[STRING_START] ||
            valid_symbols[STRING_NAME_START] ||
            valid_symbols[NODE_PATH_START])) {
        Delimiter delimiter = new_delimiter();

        bool has_flags = true;

        switch (lexer->lookahead) {
            case 'r': set_raw(&delimiter); break;
            case '&': set_name(&delimiter); break;
            case '^': set_node_path(&delimiter); break;

            // For backward compatibility with 3.x versions
            case '@': set_node_path(&delimiter); break;
            default: has_flags = false; break;
        }

        if (has_flags) advance(lexer);

        if (lexer->lookahead == '\'' || lexer->lookahead == '"') {
            handle_quote(lexer, &delimiter, lexer->lookahead);
        }

        if (end_character(&delimiter)) {
            VEC_PUSH(scanner->delimiters, delimiter);

            if (is_node_path(&delimiter)) {
                lexer->result_symbol = NODE_PATH_START;
            } else if (is_name(&delimiter)) {
                lexer->result_symbol = STRING_NAME_START;
            } else {
                lexer->result_symbol = STRING_START;
            }

            return true;
        }

        if (has_flags) {
            return false;
        }
    }

    return false;
}

unsigned tree_sitter_gdscript_external_scanner_serialize(void *payload,
                                                         char *buffer) {
    Scanner *scanner = (Scanner *)payload;

    size_t size = 0;

    size_t delimiter_count = scanner->delimiters->len;
    if (delimiter_count > UINT8_MAX) {
        delimiter_count = UINT8_MAX;
    }
    buffer[size++] = (char)delimiter_count;

    if (delimiter_count > 0) {
        memcpy(&buffer[size], scanner->delimiters->data, delimiter_count);
    }
    size += delimiter_count;

    for (int iter = 1; (uint32_t)iter < scanner->indents->len &&
                       size < TREE_SITTER_SERIALIZATION_BUFFER_SIZE;
         ++iter) {
        buffer[size++] = (char)scanner->indents->data[iter];
    }

    if (size < TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
        buffer[size++] = scanner->has_inline_comment ? 1 : 0;
    }

    return size;
}

void tree_sitter_gdscript_external_scanner_deserialize(void *payload,
                                                       const char *buffer,
                                                       unsigned length) {
    Scanner *scanner = (Scanner *)payload;

    VEC_CLEAR(scanner->delimiters);
    VEC_CLEAR(scanner->indents);
    VEC_PUSH(scanner->indents, 0);
    scanner->has_inline_comment = false;

    if (length > 0) {
        size_t size = 0;

        size_t delimiter_count = (uint8_t)buffer[size++];
        if (delimiter_count > 0) {
            VEC_GROW(scanner->delimiters, delimiter_count);
            scanner->delimiters->len = delimiter_count;
            memcpy(scanner->delimiters->data, &buffer[size], delimiter_count);
            size += delimiter_count;
        }

        // Deserialize the indents. Right now it's all except the last byte
        // which is the has_inline_comment field.
        for (; size < length - 1; size++) {
            VEC_PUSH(scanner->indents, (unsigned char)buffer[size]);
        }

        // Now we got all indents, deserialize the has_inline_comment field,
        // which is the last byte in the buffer. It's a char so we need to
        // convert it to a bool.
        if (size < length) {
            scanner->has_inline_comment = buffer[size++] != 0;
        }

        assert(size == length);
    }
}

void *tree_sitter_gdscript_external_scanner_create() {
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)
    _Static_assert(sizeof(Delimiter) == sizeof(char), "");
#else
    assert(sizeof(Delimiter) == sizeof(char));
#endif
    Scanner *scanner = calloc(1, sizeof(Scanner));
    if (!scanner) {
        // What is the tree-sitter idiomatic way to handle this?
        // fprintf(stderr, "Failed to allocate memory for scanner\n");
        return NULL;
    }

    scanner->indents = calloc(1, sizeof(indent_vec));
    scanner->delimiters = calloc(1, sizeof(delimiter_vec));
    tree_sitter_gdscript_external_scanner_deserialize(scanner, NULL, 0);
    return scanner;
}

void tree_sitter_gdscript_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    VEC_FREE(scanner->indents);
    VEC_FREE(scanner->delimiters);
    free(scanner->indents);
    free(scanner->delimiters);
    free(scanner);
}