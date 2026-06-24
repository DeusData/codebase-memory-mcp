#include "tag.h"
#include "tree_sitter/parser.h"

#include <wctype.h>

enum TokenType {
    START_TAG_NAME,
    SCRIPT_START_TAG_NAME,
    STYLE_START_TAG_NAME,
    END_TAG_NAME,
    ERRONEOUS_END_TAG_NAME,
    SELF_CLOSING_TAG_DELIMITER,
    IMPLICIT_END_TAG,
    RAW_TEXT,
    COMMENT,
    SVELTE_RAW_TEXT,
    SVELTE_RAW_TEXT_EACH,
    SVELTE_RAW_TEXT_SNIPPET_ARGUMENTS,
    AT,
    HASH,
    SLASH,
    COLON,
};

typedef struct {
    Array(Tag) tags;
} Scanner;

#define MAX(a, b) ((a) > (b) ? (a) : (b))

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static unsigned serialize(Scanner *scanner, char *buffer) {
    uint16_t tag_count = scanner->tags.size > UINT16_MAX ? UINT16_MAX : scanner->tags.size;
    uint16_t serialized_tag_count = 0;

    unsigned size = sizeof(tag_count);
    memcpy(&buffer[size], &tag_count, sizeof(tag_count));
    size += sizeof(tag_count);

    for (; serialized_tag_count < tag_count; serialized_tag_count++) {
        Tag tag = scanner->tags.contents[serialized_tag_count];
        if (tag.type == CUSTOM) {
            unsigned name_length = tag.custom_tag_name.size;
            if (name_length > UINT8_MAX) {
                name_length = UINT8_MAX;
            }
            if (size + 2 + name_length >= TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
                break;
            }
            buffer[size++] = (char)tag.type;
            buffer[size++] = (char)name_length;
            strncpy(&buffer[size], tag.custom_tag_name.contents, name_length);
            size += name_length;
        } else {
            if (size + 1 >= TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
                break;
            }
            buffer[size++] = (char)tag.type;
        }
    }

    memcpy(&buffer[0], &serialized_tag_count, sizeof(serialized_tag_count));
    return size;
}

static void deserialize(Scanner *scanner, const char *buffer, unsigned length) {
    for (unsigned i = 0; i < scanner->tags.size; i++) {
        tag_free(&scanner->tags.contents[i]);
    }
    array_clear(&scanner->tags);

    if (length > 0) {
        unsigned size = 0;
        uint16_t tag_count = 0;
        uint16_t serialized_tag_count = 0;

        memcpy(&serialized_tag_count, &buffer[size], sizeof(serialized_tag_count));
        size += sizeof(serialized_tag_count);

        memcpy(&tag_count, &buffer[size], sizeof(tag_count));
        size += sizeof(tag_count);

        array_reserve(&scanner->tags, tag_count);
        if (tag_count > 0) {
            unsigned iter = 0;
            for (iter = 0; iter < serialized_tag_count; iter++) {
                Tag tag = tag_new();
                tag.type = (TagType)buffer[size++];
                if (tag.type == CUSTOM) {
                    uint16_t name_length = (uint8_t)buffer[size++];
                    array_reserve(&tag.custom_tag_name, name_length);
                    tag.custom_tag_name.size = name_length;
                    memcpy(tag.custom_tag_name.contents, &buffer[size], name_length);
                    size += name_length;
                }
                array_push(&scanner->tags, tag);
            }
            // add zero tags if we didn't read enough, this is because the
            // buffer had no more room but we held more tags.
            for (; iter < tag_count; iter++) {
                array_push(&scanner->tags, tag_new());
            }
        }
    }
}

static String scan_tag_name(TSLexer *lexer) {
    String tag_name = array_new();
    while (iswalnum(lexer->lookahead) || lexer->lookahead == '-' || lexer->lookahead == ':' ||
           lexer->lookahead == '.') {
        // In `tree-sitter-html`, this is where each character is uppercased,
        // but we're preserving the original case. Why?
        //
        // The comparisons for HTML are case-insensitive, since browsers parse
        // HTML tag names in a case-insensitive manner. But Svelte enforces
        // that all usages of plain HTML are in lowercase! Imported Svelte
        // components, on the other hand, must have an initial capital letter.
        //
        // For the purposes of this parser, we'll enforce HTML's rules about
        // containment and void tags only on all-lowercase tag names.
        //
        // There are some hypothetical tag names that we could confidently flag
        // as invalid — element names like `inupt` that fail to meet the naming
        // requirements for both custom elements and Svelte components. We can
        // leave that for later, though.
        array_push(&tag_name, lexer->lookahead);
        advance(lexer);
    }
    return tag_name;
}

static bool scan_comment(TSLexer *lexer) {
    if (lexer->lookahead != '-') {
        return false;
    }
    advance(lexer);
    if (lexer->lookahead != '-') {
        return false;
    }
    advance(lexer);

    unsigned dashes = 0;
    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '-':
                ++dashes;
                break;
            case '>':
                if (dashes >= 2) {
                    lexer->result_symbol = COMMENT;
                    advance(lexer);
                    lexer->mark_end(lexer);
                    return true;
                }
                dashes = 0;
                break;
            default:
                dashes = 0;
        }
        advance(lexer);
    }
    return false;
}

static bool scan_javascript_template_string(TSLexer *lexer);

static bool scan_javascript_quoted_string(TSLexer *lexer, int32_t delimiter);

// After consuming a forward slash and seeing an asterisk immediately after it,
// call this function to advance the lexer to the end of the JavaScript block
// comment.
static bool scan_javascript_block_comment(TSLexer *lexer) {
    if (lexer->lookahead != '*') {
        return false;
    }
    advance(lexer);
    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '*':
                advance(lexer);
                if (lexer->lookahead == '/') {
                    advance(lexer);
                    return true;
                }
                break;
            default:
                advance(lexer);
        }
    }
    return false;
}

// After consuming a forward slash and seeing another forward slash immediately
// after it, call this function to advance the lexer to the end of the
// JavaScript line comment.
static bool scan_javascript_line_comment(TSLexer *lexer) {
    if (lexer->lookahead != '/') {
        return false;
    }
    advance(lexer);
    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '\n':
            case '\r':
                advance(lexer);
                return true;
            default:
                advance(lexer);
        }
    }
    return false;
}

// When you see a `{` in front of you in a JavaScript context, call this
// function to scan through until the next balanced (unescaped) brace.
static bool scan_javascript_balanced_brace(TSLexer *lexer) {
    if (lexer->lookahead != '{') {
        return false;
    }
    uint8_t brace_level = 0;
    advance(lexer);
    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '`':
                scan_javascript_template_string(lexer);
                break;
            case '\\':
                // Escape character. Advance twice.
                advance(lexer);
                advance(lexer);
                break;
            case '\'':
            case '"':
                scan_javascript_quoted_string(lexer, lexer->lookahead);
                break;
            case '{':
                brace_level++;
                advance(lexer);
                break;
            case '}':
                advance(lexer);
                if (brace_level == 0) {
                    return true;
                }
                brace_level--;
                break;
            default:
                advance(lexer);
        }
    }
    return false;
}

// When you see a single or double quote that starts a string, call this
// function to scan through until the end of the quoted string.
static bool scan_javascript_quoted_string(TSLexer *lexer, int32_t delimiter) {
    if (lexer->lookahead != delimiter) {
        return false;
    }
    advance(lexer);
    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '\\':
                // Escape character. Advance again.
                advance(lexer);
                advance(lexer);
                break;
            default:
                if (lexer->lookahead == delimiter) {
                    advance(lexer);
                    return true;
                }
                advance(lexer);
        }
    }
    return false;
}

// When you see a backtick in a JavaScript context, call this function to scan
// through until the end of the template string.
static bool scan_javascript_template_string(TSLexer *lexer) {
    if (lexer->lookahead != '`') {
        return false;
    }
    advance(lexer);
    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '$':
                advance(lexer);
                if (lexer->lookahead == '{') {
                    scan_javascript_balanced_brace(lexer);
                }
                break;
            case '\\':
                // Escape character. Advance again.
                advance(lexer);
                advance(lexer);
                break;
            case '`':
                advance(lexer);
                return true;
            default:
                advance(lexer);
        }
    }
    return false;
}

static bool scan_raw_text(Scanner *scanner, TSLexer *lexer) {
    if (scanner->tags.size == 0) {
        return false;
    }

    lexer->mark_end(lexer);

    const char *end_delimiter = array_back(&scanner->tags)->type == SCRIPT ? "</SCRIPT" : "</STYLE";

    unsigned delimiter_index = 0;
    while (lexer->lookahead) {
        if ((char)towupper(lexer->lookahead) == end_delimiter[delimiter_index]) {
            delimiter_index++;
            if (delimiter_index == strlen(end_delimiter)) {
                break;
            }
            advance(lexer);
        } else {
            delimiter_index = 0;
            advance(lexer);
            lexer->mark_end(lexer);
        }
    }

    lexer->result_symbol = RAW_TEXT;
    return true;
}

// Like `scan_svelte_raw_text`, but designed to operate inside the parentheses
// of a `#snippet` definition. Consumes everything until just before the next
// balanced parenthesis.
static bool scan_svelte_raw_text_snippet(TSLexer *lexer) {
    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }
    lexer->result_symbol = SVELTE_RAW_TEXT_SNIPPET_ARGUMENTS;
    uint8_t paren_level = 0;
    bool advanced_once = false;
    while (!lexer->eof(lexer)) {
        switch (lexer->lookahead) {
            case '/':
                advance(lexer);
                if (lexer->lookahead == '*') {
                    scan_javascript_block_comment(lexer);
                } else if (lexer->lookahead == '/') {
                    scan_javascript_line_comment(lexer);
                }
                break;
            case '\\':
                // Escape mode. Advance again.
                advance(lexer);
                break;
            case '"':
            case '\'':
                // A quoted string is starting. Advance past the end of the
                // closing delimiter.
                scan_javascript_quoted_string(lexer, lexer->lookahead);
                break;
            case '`':
                // A template string is starting. Advance past the end of the
                // closing delimiter.
                scan_javascript_template_string(lexer);
                break;
            case ')':
                if (paren_level == 0) {
                    lexer->mark_end(lexer);
                    return advanced_once;
                }
                advance(lexer);
                paren_level--;
                break;
            case '(':
                advance(lexer);
                paren_level++;
                break;
            default:
                advance(lexer);
                break;
        }
        advanced_once = true;
    }
    return false;
}

static bool scan_svelte_raw_text(TSLexer *lexer, const bool *valid_symbols) {
    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }

    if ((lexer->lookahead == '@' && valid_symbols[AT]) || (lexer->lookahead == '#' && valid_symbols[HASH]) ||
        (lexer->lookahead == ':' && valid_symbols[COLON])) {
        return false;
    }

    // The presence of a special Svelte sigil disqualifies this as a raw text
    // node. This helps us distinguish those nodes from things like `{:else}`.
    bool has_sigil = lexer->lookahead == '@' || lexer->lookahead == '#' || lexer->lookahead == ':';
    if (has_sigil) {
        return false;
    }

    // Keep track of whether we've advanced even once. If we haven't, then that
    // implies we've encountered `{}``, which isn't a valid `svelte_raw_text`
    // node.
    bool advanced_once = false;

    if (lexer->lookahead == '/' && valid_symbols[SLASH]) {
        advance(lexer);
        if (lexer->lookahead == '*') {
            return scan_javascript_block_comment(lexer);
        }
        if (lexer->lookahead != '/') { // JavaScript comment
            return false;
        }

        advanced_once = true;
    }

    lexer->result_symbol = valid_symbols[SVELTE_RAW_TEXT_EACH] ? SVELTE_RAW_TEXT_EACH : SVELTE_RAW_TEXT;

    uint8_t brace_level = 0;

    // We're searching for a balanced `}`, but along the way we have to
    // consider characters that might put us into contexts for which braces
    // have a different meaning. For instance: a brace inside a comment
    // shouldn't count toward brace balancing, nor should a brace inside of a
    // string.
    while (!lexer->eof(lexer)) {
        switch (lexer->lookahead) {
            case '/':
                advance(lexer);
                advanced_once = true;
                if (lexer->lookahead == '*') {
                    scan_javascript_block_comment(lexer);
                } else if (lexer->lookahead == '/') {
                    scan_javascript_line_comment(lexer);
                }
                break;
            case '\\':
                // Escape mode. Advance again.
                advance(lexer);
                advanced_once = true;
                break;
            case '"':
            case '\'':
                // A quoted string is starting. Advance past the end of the
                // closing delimiter.
                scan_javascript_quoted_string(lexer, lexer->lookahead);
                advanced_once = true;
                break;
            case '`':
                // A template string is starting. Advance past the end of the
                // closing delimiter.
                scan_javascript_template_string(lexer);
                advanced_once = true;
                break;
            case '}':
                if (brace_level == 0) {
                    lexer->mark_end(lexer);
                    return advanced_once;
                }
                advance(lexer);
                brace_level--;
                advanced_once = true;
                break;

            case '{':
                advance(lexer);
                brace_level++;
                advanced_once = true;
                break;

            case 'a':
                if (lexer->result_symbol == SVELTE_RAW_TEXT_EACH) {
                    lexer->mark_end(lexer);
                    advance(lexer);
                    advanced_once = true;
                    if (lexer->lookahead == 's') {
                        advance(lexer);
                        if (iswspace(lexer->lookahead)) {
                            return advanced_once;
                        }
                    }
                } else {
                    advance(lexer);
                    advanced_once = true;
                }
                break;

            default:
                advance(lexer);
                advanced_once = true;
                break;
        }
    }

    return false;
}

static inline void pop_tag(Scanner *scanner) {
    Tag popped_tag = array_pop(&scanner->tags);
    tag_free(&popped_tag);
}

static bool scan_implicit_end_tag(Scanner *scanner, TSLexer *lexer) {
    Tag *parent = scanner->tags.size == 0 ? NULL : array_back(&scanner->tags);

    bool is_closing_tag = false;
    if (lexer->lookahead == '/') {
        is_closing_tag = true;
        advance(lexer);
    } else {
        if (parent && tag_is_void(parent)) {
            pop_tag(scanner);
            lexer->result_symbol = IMPLICIT_END_TAG;
            return true;
        }
    }

    String tag_name = scan_tag_name(lexer);
    if (tag_name.size == 0 && !lexer->eof(lexer)) {
        array_delete(&tag_name);
        return false;
    }

    Tag next_tag = tag_for_name(tag_name);

    if (is_closing_tag) {
        // The tag correctly closes the topmost element on the stack
        if (scanner->tags.size > 0 && tag_eq(array_back(&scanner->tags), &next_tag)) {
            tag_free(&next_tag);
            return false;
        }

        // Otherwise, dig deeper and queue implicit end tags (to be nice in
        // the case of malformed Svelte)
        for (unsigned i = scanner->tags.size; i > 0; i--) {
            if (scanner->tags.contents[i - 1].type == next_tag.type) {
                pop_tag(scanner);
                lexer->result_symbol = IMPLICIT_END_TAG;
                tag_free(&next_tag);
                return true;
            }
        }
    } else if (parent &&
               (!tag_can_contain(parent, &next_tag) ||
                ((parent->type == HTML || parent->type == HEAD || parent->type == BODY) && lexer->eof(lexer)))) {
        pop_tag(scanner);
        lexer->result_symbol = IMPLICIT_END_TAG;
        tag_free(&next_tag);
        return true;
    }

    tag_free(&next_tag);
    return false;
}

static bool scan_start_tag_name(Scanner *scanner, TSLexer *lexer) {
    String tag_name = scan_tag_name(lexer);

    if (tag_name.size == 0) {
        array_delete(&tag_name);
        return false;
    }

    Tag tag = tag_for_name(tag_name);
    array_push(&scanner->tags, tag);
    switch (tag.type) {
        case SCRIPT:
            lexer->result_symbol = SCRIPT_START_TAG_NAME;
            break;
        case STYLE:
            lexer->result_symbol = STYLE_START_TAG_NAME;
            break;
        default:
            lexer->result_symbol = START_TAG_NAME;
            break;
    }

    return true;
}

static bool scan_end_tag_name(Scanner *scanner, TSLexer *lexer) {
    String tag_name = scan_tag_name(lexer);

    if (tag_name.size == 0) {
        array_delete(&tag_name);
        return false;
    }

    Tag tag = tag_for_name(tag_name);
    if (scanner->tags.size > 0 && tag_eq(array_back(&scanner->tags), &tag)) {
        pop_tag(scanner);
        lexer->result_symbol = END_TAG_NAME;
    } else {
        lexer->result_symbol = ERRONEOUS_END_TAG_NAME;
    }

    tag_free(&tag);
    return true;
}

static bool scan_self_closing_tag_delimiter(Scanner *scanner, TSLexer *lexer) {
    advance(lexer);
    if (lexer->lookahead == '>') {
        advance(lexer);
        if (scanner->tags.size > 0) {
            pop_tag(scanner);
            lexer->result_symbol = SELF_CLOSING_TAG_DELIMITER;
        }
        return true;
    }
    return false;
}

static bool scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
    if (valid_symbols[RAW_TEXT] && !valid_symbols[START_TAG_NAME] && !valid_symbols[END_TAG_NAME]) {
        return scan_raw_text(scanner, lexer);
    }

    if (valid_symbols[SVELTE_RAW_TEXT_SNIPPET_ARGUMENTS]) {
        return scan_svelte_raw_text_snippet(lexer);
    }

    if (valid_symbols[SVELTE_RAW_TEXT] || valid_symbols[SVELTE_RAW_TEXT_EACH]) {
        return scan_svelte_raw_text(lexer, valid_symbols);
    }

    while (iswspace(lexer->lookahead)) {
        skip(lexer);
    }

    switch (lexer->lookahead) {
        case '<':
            lexer->mark_end(lexer);
            advance(lexer);

            if (lexer->lookahead == '!') {
                advance(lexer);
                return scan_comment(lexer);
            }

            if (valid_symbols[IMPLICIT_END_TAG]) {
                return scan_implicit_end_tag(scanner, lexer);
            }
            break;

        case '{':
        case '\0':
            if (valid_symbols[IMPLICIT_END_TAG]) {
                return scan_implicit_end_tag(scanner, lexer);
            }
            break;

        case '/':
            if (valid_symbols[SELF_CLOSING_TAG_DELIMITER]) {
                return scan_self_closing_tag_delimiter(scanner, lexer);
            }
            break;

        default:
            if ((valid_symbols[START_TAG_NAME] || valid_symbols[END_TAG_NAME]) && !valid_symbols[RAW_TEXT]) {
                return valid_symbols[START_TAG_NAME] ? scan_start_tag_name(scanner, lexer)
                                                     : scan_end_tag_name(scanner, lexer);
            }
    }

    return false;
}

void *tree_sitter_svelte_external_scanner_create() {
    Scanner *scanner = (Scanner *)ts_calloc(1, sizeof(Scanner));
    return scanner;
}

bool tree_sitter_svelte_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
    Scanner *scanner = (Scanner *)payload;
    return scan(scanner, lexer, valid_symbols);
}

unsigned tree_sitter_svelte_external_scanner_serialize(void *payload, char *buffer) {
    Scanner *scanner = (Scanner *)payload;
    return serialize(scanner, buffer);
}

void tree_sitter_svelte_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
    Scanner *scanner = (Scanner *)payload;
    deserialize(scanner, buffer, length);
}

void tree_sitter_svelte_external_scanner_destroy(void *payload) {
    Scanner *scanner = (Scanner *)payload;
    for (unsigned i = 0; i < scanner->tags.size; i++) {
        tag_free(array_get(&scanner->tags, i));
    }
    array_delete(&scanner->tags);
    ts_free(scanner);
}