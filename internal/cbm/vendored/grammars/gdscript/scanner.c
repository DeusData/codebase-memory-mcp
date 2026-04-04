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
    CLOSE_PAREN,
    CLOSE_BRACKET,
    CLOSE_BRACE,
    COMMA,
    /* COLON, // See grammar.js externals */
    BODY_END,
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
} Scanner;

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

/**
 * Check if the current lexer position matches a specific word.
 * The lexer position is restored after checking.
 * 
 * @param lexer The lexer to check
 * @param word The word to match against
 * @return true if the word matches, false otherwise
 */
static bool lookahead_string(TSLexer *lexer, const char *string) {
    const char *cursor = string;
    
    while (*cursor && lexer->lookahead == *cursor) {
        skip(lexer);
        cursor++;
    }
    
    bool matches = (*cursor == '\0');
    
    return matches;
}

/**
 * Skip whitespace characters and update indentation tracking.
 * 
 * @param lexer The lexer to advance
 * @param indent_length Pointer to current indentation length (modified in place)
 * @param found_end_of_line Pointer to end-of-line flag (set to true if newline encountered), can be NULL
 * @return true if a whitespace character was processed, false otherwise
 * 
 * Handles:
 * - ' ' (space): increments indent_length
 * - '\t' (tab): adds 8 to indent_length
 * - '\n' (newline): resets indent_length to 0, sets found_end_of_line to true (if not NULL)
 * - '\r', '\f' (carriage return, form feed): resets indent_length to 0
 */
static inline bool skip_whitespace(TSLexer *lexer, uint32_t *indent_length, bool *found_end_of_line) {
    if (lexer->lookahead == '\n') {
        if (found_end_of_line) {
            *found_end_of_line = true;
        }

        *indent_length = 0;
        skip(lexer);

        return true;
    } else if (lexer->lookahead == ' ') {
        (*indent_length)++;
        skip(lexer);

        return true;
    } else if (lexer->lookahead == '\r' || lexer->lookahead == '\f') {
        *indent_length = 0;
        skip(lexer);

        return true;
    } else if (lexer->lookahead == '\t') {
        *indent_length += 8;
        skip(lexer);

        return true;
    }

    return false;
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
            }
            advance(lexer);
            has_content = true;
        }
    }

    lexer->mark_end(lexer);



    bool found_end_of_line = false;
    uint32_t indent_length = 0;

    // Track the indentation level of the most recent line that contained actual content
    // (comments or code statements). This is used to prevent premature DEDENT emission
    // when empty lines appear between content at the same indentation level, and to
    // maintain proper scope association for comments that appear at the end of blocks.
    uint32_t last_non_empty_indent = 0;

    for (;;) {
        if (skip_whitespace(lexer, &indent_length, &found_end_of_line)) {
            continue;
        } else if (lexer->lookahead == '#') {
            // The current scanner can scan past a line return into a comment.
            // In that case we want to stop processing here, since it means
            // we're looking potentially at a comment on the next line compared
            // to the starting point of this scan.
            if (!found_end_of_line) {
                break;
            }

            last_non_empty_indent = indent_length;

            // Store the comment's indentation for special handling of dedented comments
            // for example:
            // ```
            // func example():
            //     # This comment is indented to the function's level
            // # this comment is dedented to column 0 but should be associated with the function
            //     var x = 10
            // # This comment is dedented to column 0 but should be associated with the function
            // ```
            uint32_t comment_indent_length = indent_length;
            
            // Check if this is a region marker - they should not be adjusted
            bool is_region_marker = false;
            if (comment_indent_length == 0) {
                // Check for #region or #endregion at column 0
                skip(lexer); // skip #
                
                // Check if the next characters are "region" or "endregion"
                if (lexer->lookahead == 'r') {
                    is_region_marker = lookahead_string(lexer, "region");
                } else if (lexer->lookahead == 'e') {
                    is_region_marker = lookahead_string(lexer, "endregion");
                }
            }
            
            // For dedented comments, adjust indentation to ensure they are parsed within the correct scope
            // Only adjust regular comments (not region markers) that are at column 0 when we're inside a function
            // AND only if this appears to be a stray comment within the function scope rather than a top-level comment
            if (!is_region_marker && scanner->indents->len > 1 && comment_indent_length == 0) {
                // Get the function-level indentation (assuming it's the first indent)
                uint16_t function_indent_length = scanner->indents->data[1];
                // Only adjust if we're inside a function (function_indent_length > 0)
                // AND this is likely a comment that belongs to the function (not a top-level docstring)
                if (function_indent_length > 0) {
                    // Look ahead to see what comes after this comment
                    
                    // Skip the comment line
                    while (lexer->lookahead && lexer->lookahead != '\n') {
                        skip(lexer);
                    }

                    if (lexer->lookahead == '\n') {
                        skip(lexer);
                    }
                    
                    // Skip whitespace and see what's next
                    uint32_t next_indent = 0;
                    while (skip_whitespace(lexer, &next_indent, NULL)) {
                        // continue
                    }
                    
                    // Only adjust if the next content is NOT at the top level (indent 0)
                    // This prevents docstring-style comments from being pulled into functions
                    if (next_indent > 0) {
                        // This is a dedented comment at column 0 inside a function
                        indent_length = function_indent_length;
                    }
                }
            }

            // Don't consume the comment - let the grammar handle it as a token
            break;
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
            // At EOF, use the last non-empty line's indentation if we haven't seen content
            // this prevents dedenting to 0 at EOF if the last line is empty
            if (last_non_empty_indent > 0) {
                indent_length = last_non_empty_indent;
            }
            
            if (scanner->indents->len > 0) {
                uint16_t current_indent_length = VEC_BACK(scanner->indents);
                if (indent_length != current_indent_length) {
                    indent_length = 0;
                }
            } else {
                indent_length = 0;
            }
            found_end_of_line = true;
            break;
        } else if (lexer->lookahead == '\n') {
            // Empty line, skip it and continue
            skip(lexer);
            indent_length = 0;
        } else {
            if (indent_length == 0 && last_non_empty_indent > 0) {
                // We're at content at level 0, but we had non-empty content at a higher level
                // Check if we should defer DEDENT
                if (scanner->indents->len > 0) {
                    uint16_t current_indent_length = VEC_BACK(scanner->indents);
                    if (last_non_empty_indent == current_indent_length ) {
                        // We had comments at the current indent level, don't dedent immediately
                        return false;
                    }
                }
            }
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

            if (valid_symbols[DEDENT] && indent_length < current_indent_length) {
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

    if (valid_symbols[STRING_START] ||
        valid_symbols[STRING_NAME_START] ||
        valid_symbols[NODE_PATH_START]) {
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

    return size;
}

void tree_sitter_gdscript_external_scanner_deserialize(void *payload,
                                                       const char *buffer,
                                                       unsigned length) {
    Scanner *scanner = (Scanner *)payload;

    VEC_CLEAR(scanner->delimiters);
    VEC_CLEAR(scanner->indents);
    VEC_PUSH(scanner->indents, 0);

    if (length > 0) {
        size_t size = 0;

        size_t delimiter_count = (uint8_t)buffer[size++];
        if (delimiter_count > 0) {
            VEC_GROW(scanner->delimiters, delimiter_count);
            scanner->delimiters->len = delimiter_count;
            memcpy(scanner->delimiters->data, &buffer[size], delimiter_count);
            size += delimiter_count;
        }

        // Deserialize the indents
        for (; size < length; size++) {
            VEC_PUSH(scanner->indents, (unsigned char)buffer[size]);
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
