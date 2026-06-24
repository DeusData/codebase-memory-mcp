#include "tag.h"
#include "tree_sitter/parser.h"

#include <stdint.h>
#include <string.h>
#include <wctype.h>

enum TokenType {
    AUTOMATIC_SEMICOLON,
    TERNARY_QMARK,
    ELVIS_OPERATOR,
    LOGICAL_OR,
    CF_START_TAG_NAME,
    CF_END_TAG_NAME,
    ERRONEOUS_CF_END_TAG_NAME,
    CF_SELF_CLOSING_TAG_DELIMITER,
    CF_SELF_CLOSING_VOID_TAG_DELIMITER,
    IMPLICIT_CF_END_TAG,
    RAW_TEXT,
    CFML_COMMENT,
    CLOSE_TAG_DELIM,
    CLOSE_CF_TAG_DELIM,
    HTML_TEXT,
    CF_VOID_START_TAG_NAME,
    CF_SET_START_TAG_NAME,
    CF_IF_START_TAG_NAME,
    CF_IF_END_TAG_NAME,
    CF_ELSEIF_TAG_NAME,
    CF_ELSE_TAG_NAME,
    CF_RETURN_START_TAG_NAME,
    CF_OUTPUT_START_TAG_NAME,

    // Rules below this point are not included by the cfquery dialect
    SCRIPT_START_TAG_NAME,
    STYLE_START_TAG_NAME,
    START_TAG_NAME,
    END_TAG_NAME,
    ERRONEOUS_END_TAG_NAME,
    SELF_CLOSING_TAG_DELIMITER,
    IMPLICIT_END_TAG,

    START_HASH_EXPRESSION,
    SINGLE_HASH,
    HASH_EMPTY,

    CF_XML_START_TAG_NAME,
    CF_XML_END_TAG_NAME,
    CF_XML_CONTENT,

    CF_QUERY_START_TAG_NAME,
    CF_QUERY_END_TAG_NAME,
    CF_QUERY_CONTENT,

    CF_SCRIPT_START_TAG_NAME,
    CF_SCRIPT_END_TAG_NAME,
    CF_SCRIPT_CONTENT,

    CF_SAVECONTENT_START_TAG_NAME,
    CF_SAVECONTENT_END_TAG_NAME,
    CF_SAVECONTENT_BODY_CFML,
    CF_SAVECONTENT_BODY_HTML,
    CF_SAVECONTENT_BODY_SCRIPT,
    CF_SAVECONTENT_BODY_CSS,
    CF_SAVECONTENT_BODY_XML,
    CF_SAVECONTENT_BODY_SQL,
    CF_SAVECONTENT_BODY_RAW,
    CF_SAVECONTENT_CONTENT,

    CF_FUNCTION_START_TAG_NAME,
    CF_FUNCTION_END_TAG_NAME,

    CF_COMPONENT_START_TAG_NAME,
    CF_COMPONENT_END_TAG_NAME,

    CF_COMPONENT_CONTENT,
    SCANNER_SYMBOL_COUNT
};

typedef struct {
    Array(Tag) tags;
    Array(Tag) cf_tags;
    uint16_t cfoutput_depth;
    uint16_t cfcomponent_depth;
    uint16_t cffunction_depth;
} Scanner;

typedef enum {
    REJECT,     // Semicolon is illegal, ie a syntax error occurred
    NO_NEWLINE, // Unclear if semicolon will be legal, continue
    ACCEPT,     // Semicolon is legal, assuming a comment was encountered
} WhitespaceResult;

#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define MAX_CF_END_DELIMITER_SIZE 256

#define VS(vs, sym, count) ((unsigned)(sym) < (count) && (vs)[(sym)])

static inline void advance(TSLexer *lexer) { lexer->advance(lexer, false); }

static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static inline bool tag_has_name(TagType type, bool is_cfquery_context) {
    return type == CUSTOM || type == CFML || type == CF_VOID || type == CF_SET ||
           type == CF_XML || type == CF_SCRIPT || type == CF_SAVECONTENT ||
           type == CF_QUERY || type == CF_OUTPUT || type == CF_FUNCTION || type == CF_RETURN ||
           type == CF_IF || type == CF_ELSEIF || type == CF_ELSE;
}

static inline bool valid_start_tag_name(const bool *vs, unsigned count) {
    return VS(vs, START_TAG_NAME, count) || VS(vs, SCRIPT_START_TAG_NAME, count) ||
           VS(vs, CF_START_TAG_NAME, count) ||
           VS(vs, CF_SET_START_TAG_NAME, count) || VS(vs, CF_VOID_START_TAG_NAME, count) ||
           VS(vs, CF_RETURN_START_TAG_NAME, count) || VS(vs, CF_XML_START_TAG_NAME, count) ||
           VS(vs, CF_QUERY_START_TAG_NAME, count) || VS(vs, CF_SCRIPT_START_TAG_NAME, count) ||
           VS(vs, CF_SAVECONTENT_START_TAG_NAME, count) || VS(vs, CF_OUTPUT_START_TAG_NAME, count) ||
           VS(vs, CF_FUNCTION_START_TAG_NAME, count) ||
           VS(vs, CF_COMPONENT_START_TAG_NAME, count) ||
           VS(vs, CF_IF_START_TAG_NAME, count) || VS(vs, CF_ELSEIF_TAG_NAME, count) ||
           VS(vs, CF_ELSE_TAG_NAME, count);
}

static inline bool valid_end_tag_name(const bool *vs, unsigned count) {
    return VS(vs, END_TAG_NAME, count) || VS(vs, CF_END_TAG_NAME, count) ||
           VS(vs, CF_XML_END_TAG_NAME, count) || VS(vs, CF_QUERY_END_TAG_NAME, count) ||
           VS(vs, CF_SCRIPT_END_TAG_NAME, count) || VS(vs, CF_SAVECONTENT_END_TAG_NAME, count) ||
           VS(vs, CF_FUNCTION_END_TAG_NAME, count) ||
           VS(vs, CF_IF_END_TAG_NAME, count);
}

static inline bool valid_cf_start_tag_name(const bool *vs, unsigned count) {
    return VS(vs, CF_START_TAG_NAME, count) || VS(vs, CF_SET_START_TAG_NAME, count) ||
           VS(vs, CF_VOID_START_TAG_NAME, count) || VS(vs, CF_RETURN_START_TAG_NAME, count) ||
           VS(vs, CF_XML_START_TAG_NAME, count) || VS(vs, CF_QUERY_START_TAG_NAME, count) ||
           VS(vs, CF_SCRIPT_START_TAG_NAME, count) || VS(vs, CF_SAVECONTENT_START_TAG_NAME, count) ||
           VS(vs, CF_OUTPUT_START_TAG_NAME, count) || VS(vs, CF_FUNCTION_START_TAG_NAME, count) ||
           VS(vs, CF_COMPONENT_START_TAG_NAME, count) ||
           VS(vs, CF_IF_START_TAG_NAME, count) || VS(vs, CF_ELSEIF_TAG_NAME, count) ||
           VS(vs, CF_ELSE_TAG_NAME, count);
}

static inline bool valid_cf_end_tag_name(const bool *vs, unsigned count) {
    return VS(vs, CF_END_TAG_NAME, count) || VS(vs, CF_XML_END_TAG_NAME, count) ||
           VS(vs, CF_QUERY_END_TAG_NAME, count) || VS(vs, CF_SCRIPT_END_TAG_NAME, count) ||
           VS(vs, CF_SAVECONTENT_END_TAG_NAME, count) || VS(vs, CF_FUNCTION_END_TAG_NAME, count) ||
           VS(vs, CF_IF_END_TAG_NAME, count);
}

static inline bool no_content_symbols(const bool *vs, unsigned count) {
    return !VS(vs, RAW_TEXT, count) && !VS(vs, CF_XML_CONTENT, count) &&
           !VS(vs, CF_QUERY_CONTENT, count) && !VS(vs, CF_SCRIPT_CONTENT, count);
}

static inline bool implicit_cf_end_tag_valid(const bool *vs, unsigned count) {
    return VS(vs, IMPLICIT_CF_END_TAG, count) && !VS(vs, CF_XML_END_TAG_NAME, count) &&
           !VS(vs, CF_QUERY_END_TAG_NAME, count) && !VS(vs, CF_SCRIPT_END_TAG_NAME, count) &&
           !VS(vs, CF_SAVECONTENT_END_TAG_NAME, count) && !VS(vs, CF_FUNCTION_END_TAG_NAME, count) &&
           !VS(vs, CF_IF_END_TAG_NAME, count) &&
           !VS(vs, CF_ELSEIF_TAG_NAME, count) && !VS(vs, CF_ELSE_TAG_NAME, count);
}

#define SERIALIZE_TAGS(tags_field, buffer, size, is_cfquery_context) do { \
    uint16_t _count = (tags_field).size > UINT16_MAX ? UINT16_MAX : (tags_field).size; \
    uint16_t _serialized = 0; \
    unsigned _count_offset = (size); \
    if ((size) + sizeof(_count) + sizeof(_count) > TREE_SITTER_SERIALIZATION_BUFFER_SIZE) break; \
    (size) += sizeof(_count); \
    (size) += sizeof(_count); \
    for (; _serialized < _count; _serialized++) { \
        Tag _tag = (tags_field).contents[_serialized]; \
        if (tag_has_name(_tag.type, is_cfquery_context)) { \
            unsigned _len = _tag.tag_name.size; \
            if (_len > UINT8_MAX) _len = UINT8_MAX; \
            if ((size) + 2 + _len + sizeof(_tag.html_depth) >= TREE_SITTER_SERIALIZATION_BUFFER_SIZE) break; \
            (buffer)[(size)++] = (char)_tag.type; \
            (buffer)[(size)++] = (char)_len; \
            strncpy(&(buffer)[(size)], _tag.tag_name.contents, _len); \
            (size) += _len; \
            memcpy(&(buffer)[(size)], &_tag.html_depth, sizeof(_tag.html_depth)); \
            (size) += sizeof(_tag.html_depth); \
        } else { \
            if ((size) + 1 >= TREE_SITTER_SERIALIZATION_BUFFER_SIZE) break; \
            (buffer)[(size)++] = (char)_tag.type; \
        } \
    } \
    memcpy(&(buffer)[_count_offset], &_serialized, sizeof(_serialized)); \
    memcpy(&(buffer)[_count_offset + sizeof(_serialized)], &_count, sizeof(_count)); \
} while(0)

static unsigned serialize(Scanner *scanner, char *buffer, bool is_cfquery_context) {
    unsigned size = 0;
    SERIALIZE_TAGS(scanner->tags, buffer, size, is_cfquery_context);
    SERIALIZE_TAGS(scanner->cf_tags, buffer, size, is_cfquery_context);
    if (size + sizeof(scanner->cfoutput_depth) + sizeof(scanner->cfcomponent_depth) + sizeof(scanner->cffunction_depth) < TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
        memcpy(&buffer[size], &scanner->cfoutput_depth, sizeof(scanner->cfoutput_depth));
        size += sizeof(scanner->cfoutput_depth);
        memcpy(&buffer[size], &scanner->cfcomponent_depth, sizeof(scanner->cfcomponent_depth));
        size += sizeof(scanner->cfcomponent_depth);
        memcpy(&buffer[size], &scanner->cffunction_depth, sizeof(scanner->cffunction_depth));
        size += sizeof(scanner->cffunction_depth);
    }
    return size;
}

#define DESERIALIZE_TAGS(tags_field, buffer, size, is_cfquery_context) do { \
    for (unsigned _i = 0; _i < (tags_field).size; _i++) tag_free(&(tags_field).contents[_i]); \
    array_clear(&(tags_field)); \
    uint16_t _serialized = 0, _count = 0; \
    memcpy(&_serialized, &(buffer)[(size)], sizeof(_serialized)); (size) += sizeof(_serialized); \
    memcpy(&_count, &(buffer)[(size)], sizeof(_count)); (size) += sizeof(_count); \
    array_reserve(&(tags_field), _count); \
    unsigned _iter = 0; \
    for (_iter = 0; _iter < _serialized; _iter++) { \
        Tag _tag = tag_new(); \
        _tag.type = (TagType)(unsigned char)(buffer)[(size)++]; \
        if (tag_has_name(_tag.type, is_cfquery_context)) { \
            uint16_t _len = (uint8_t)(buffer)[(size)++]; \
            array_reserve(&_tag.tag_name, _len); \
            _tag.tag_name.size = _len; \
            memcpy(_tag.tag_name.contents, &(buffer)[(size)], _len); \
            (size) += _len; \
            memcpy(&_tag.html_depth, &(buffer)[(size)], sizeof(_tag.html_depth)); \
            (size) += sizeof(_tag.html_depth); \
        } \
        array_push(&(tags_field), _tag); \
    } \
    for (; _iter < _count; _iter++) array_push(&(tags_field), tag_new()); \
} while(0)

static void deserialize(Scanner *scanner, const char *buffer, unsigned length, bool is_cfquery_context) {
    scanner->cfoutput_depth = 0;
    scanner->cfcomponent_depth = 0;
    scanner->cffunction_depth = 0;
    if (length > 0) {
        unsigned size = 0;
        DESERIALIZE_TAGS(scanner->tags, buffer, size, is_cfquery_context);
        DESERIALIZE_TAGS(scanner->cf_tags, buffer, size, is_cfquery_context);
        if (size + sizeof(scanner->cfoutput_depth) <= length) {
            memcpy(&scanner->cfoutput_depth, &buffer[size], sizeof(scanner->cfoutput_depth));
            size += sizeof(scanner->cfoutput_depth);
        }
        if (size + sizeof(scanner->cfcomponent_depth) <= length) {
            memcpy(&scanner->cfcomponent_depth, &buffer[size], sizeof(scanner->cfcomponent_depth));
            size += sizeof(scanner->cfcomponent_depth);
        }
        if (size + sizeof(scanner->cffunction_depth) <= length) {
            memcpy(&scanner->cffunction_depth, &buffer[size], sizeof(scanner->cffunction_depth));
            size += sizeof(scanner->cffunction_depth);
        }
    } else {
        for (unsigned i = 0; i < scanner->tags.size; i++) tag_free(&scanner->tags.contents[i]);
        array_clear(&scanner->tags);
        for (unsigned i = 0; i < scanner->cf_tags.size; i++) tag_free(&scanner->cf_tags.contents[i]);
        array_clear(&scanner->cf_tags);
    }
}

typedef struct {
    String tag_name;
    bool is_cf_tag;
} TagNameResult;

static TagNameResult scan_tag_name(TSLexer *lexer, bool is_cfquery_context) {
    TagNameResult result;
    String tag_name = array_new();
    bool is_cf_tag = false;

    // ColdFusion tags might start with 'C', ie. <cfcomponent or <cfcontinue
    if ( lexer->lookahead == 'c' || lexer->lookahead == 'C' ) {
        array_push(&tag_name, towupper(lexer->lookahead));
        advance(lexer);
        if (lexer->lookahead == 'f' || lexer->lookahead == 'F') {
            is_cf_tag = true;
            array_delete(&tag_name);
            advance(lexer);
        }
    }

    while (( iswalnum(lexer->lookahead) || lexer->lookahead == '-' || lexer->lookahead == '_' || lexer->lookahead == ':' )) {
        array_push(&tag_name, towupper(lexer->lookahead));
        advance(lexer);
    }

    result.tag_name = tag_name;
    result.is_cf_tag = is_cf_tag;

    return result;
}

static bool scan_comment(TSLexer *lexer, bool is_cfquery_context) {
    if (lexer->lookahead != '-') {
        return false;
    }
    advance(lexer);
    if (lexer->lookahead != '-') {
        return false;
    }

    advance(lexer);

    // IE conditional comments: <!--[if ...]>...<![endif]--> or <!--<![endif]-->
    // These don't nest — just scan until -->
    if (lexer->lookahead == '[' || lexer->lookahead == '<') {
        unsigned close_dashes = 0;
        while (lexer->lookahead) {
            if (lexer->lookahead == '-') {
                close_dashes++;
            } else if (lexer->lookahead == '>' && close_dashes >= 2) {
                lexer->result_symbol = CFML_COMMENT;
                advance(lexer);
                lexer->mark_end(lexer);
                return true;
            } else {
                close_dashes = 0;
            }
            advance(lexer);
        }
        return false;
    }

    unsigned dashes = 0;
    unsigned direction = -1;
    unsigned nesting = 1;

    while (lexer->lookahead) {
        switch (lexer->lookahead) {
            case '-':
                ++dashes;
                if ( direction == 1 && dashes >= 2 ) {
                    ++nesting;
                    direction = -1;
                    dashes = 0;
                }
                break;
            case '>':
                if (dashes >= 2) {
                    --nesting;
                    lexer->result_symbol = CFML_COMMENT;
                    advance(lexer);
                    lexer->mark_end(lexer);
                    if ( nesting == 0 ) {
                        return true;
                    }
                    dashes = 0;
                    direction = -1;
                    continue;
                }
                direction = -1;
                dashes = 0;
                break;
            case '<':
                direction = 0;
                dashes = 0;
                break;
            case '!':
                if ( direction == 0 ) {
                    direction = 1;
                    break;
                }
                direction = -1;
                dashes = 0;
                break;
            default:
                direction = -1;
                dashes = 0;
                break;
        }
        advance(lexer);
    }
    return false;
}

static WhitespaceResult scan_whitespace_and_comments(TSLexer *lexer, bool *scanned_comment, bool consume, bool is_cfquery_context) {
    bool saw_block_newline = false;
    for (;;) {
        while (iswspace(lexer->lookahead)) {
            skip(lexer);
        }

        if (lexer->lookahead == '/') {
            skip(lexer);

            if (lexer->lookahead == '/') {
                skip(lexer);
                while (lexer->lookahead != 0 && lexer->lookahead != '\n' && lexer->lookahead != 0x2028 && lexer->lookahead != 0x2029) {
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


static bool scan_html_text(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {
    // Check if we're inside a script/style tag
    bool in_script_style = false;
    if (scanner->tags.size > 0) {
        TagType type = array_back(&scanner->tags)->type;
        if (type == SCRIPT || type == STYLE) {
            in_script_style = true;
        }
    }

    // saw_text will be true if we see any non-whitespace content, or any whitespace content that is not a newline and
    // does not immediately follow a newline.
    bool saw_text = false;
    // at_newline will be true if we are currently at a newline, or if we are at whitespace that is not a newline but
    // immediately follows a newline.
    bool at_newline = false;

    bool saw_any = false;

    if (in_script_style) {
        // Inside script/style: consume until #, <cf, </cf, or </script|</style
        lexer->mark_end(lexer);
        while (lexer->lookahead != 0 && lexer->lookahead != '#') {
            if (lexer->lookahead == '<') {
                // Peek for <cf or </cf or </script|</style
                advance(lexer);
                if (towupper(lexer->lookahead) == 'C') {
                    advance(lexer);
                    if (towupper(lexer->lookahead) == 'F') {
                        break;
                    }
                    lexer->mark_end(lexer);
                    saw_text = true;
                    saw_any = true;
                    continue;
                } else if (lexer->lookahead == '/') {
                    advance(lexer);
                    if (towupper(lexer->lookahead) == 'C') {
                        advance(lexer);
                        if (towupper(lexer->lookahead) == 'F') {
                            break;
                        }
                        lexer->mark_end(lexer);
                        saw_text = true;
                        saw_any = true;
                        continue;
                    } else if (towupper(lexer->lookahead) == 'S') {
                        // Potential </script or </style - stop
                        break;
                    }
                    lexer->mark_end(lexer);
                    saw_text = true;
                    saw_any = true;
                    continue;
                }
                lexer->mark_end(lexer);
                saw_text = true;
                saw_any = true;
                continue;
            }
            saw_text = true;
            saw_any = true;
            advance(lexer);
            lexer->mark_end(lexer);
        }
        lexer->result_symbol = HTML_TEXT;
        return saw_text || (saw_any && lexer->lookahead == '#');
    }

    while (lexer->lookahead != 0 && lexer->lookahead != '<' && lexer->lookahead != '>' && lexer->lookahead != '{' &&
           lexer->lookahead != '}' && lexer->lookahead != '#') {
        if (lexer->lookahead == '&') {
            // Peek ahead to determine if this is an entity
            lexer->mark_end(lexer);
            advance(lexer);
            if (lexer->lookahead == '#') {
                // Could be numeric entity (&#digits; or &#xhex;) or &# followed by CFML hash
                // Mark end after & so it's consumed as text
                lexer->mark_end(lexer);
                saw_text = true;
                saw_any = true;
                advance(lexer);
                if (lexer->lookahead == 'x' || lexer->lookahead == 'X' || iswdigit(lexer->lookahead)) {
                    // Numeric entity - consume fully as text
                    if (lexer->lookahead == 'x' || lexer->lookahead == 'X') {
                        advance(lexer);
                        while (iswxdigit(lexer->lookahead)) advance(lexer);
                    } else {
                        while (iswdigit(lexer->lookahead)) advance(lexer);
                    }
                    if (lexer->lookahead == ';') advance(lexer);
                    lexer->mark_end(lexer);
                    continue;
                }
                // &# not followed by digit/x - & consumed as text, break at #
                break;
            }
            if (iswalpha(lexer->lookahead)) {
                // Could be &word; - scan ahead for ;
                unsigned count = 0;
                while (iswalpha(lexer->lookahead) && count < 31) {
                    advance(lexer);
                    count++;
                }
                if (lexer->lookahead == ';' && count > 0) {
                    // Valid entity pattern - stop before &
                    break;
                }
                // Not a valid entity - consume as text
                lexer->mark_end(lexer);
                saw_text = true;
                saw_any = true;
                continue;
            }
            // & followed by non-alpha, non-# - consume as text
            lexer->mark_end(lexer);
            saw_text = true;
            saw_any = true;
            continue;
        }
        bool is_wspace = iswspace(lexer->lookahead);
        if (lexer->lookahead == '\n') {
            at_newline = true;
        } else {
            at_newline &= is_wspace;
            if (!at_newline) {
                saw_text = true;
            }
        }
        saw_any = true;
        advance(lexer);
    }

    lexer->result_symbol = HTML_TEXT;
    // Emit whitespace-only content when the next char is '#' so the parser
    // advances to a state where hash tokens become valid.
    return saw_text || (saw_any && lexer->lookahead == '#');
}


static bool scan_script_comment(TSLexer *lexer, bool is_cfquery_context) {

    for (;;) {

        if (lexer->lookahead == '/') {
            skip(lexer);
            while (lexer->lookahead != 0 && lexer->lookahead != '\n' && lexer->lookahead != 0x2028 &&
                    lexer->lookahead != 0x2029) {
                skip(lexer);
            }
            //*scanned_comment = true;
        } else if (lexer->lookahead == '*') {
            skip(lexer);
            while (lexer->lookahead != 0) {
                if (lexer->lookahead == '*') {
                    skip(lexer);
                    if (lexer->lookahead == '/') {
                        skip(lexer);
                        //*scanned_comment = true;
                        break;
                    }
                } else {
                    skip(lexer);
                }
            }
        } else {
            return false;
        }
    }
}


static bool scan_cfquery_content(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {

    if (scanner->cf_tags.size == 0) {
        return false;
    }

    Tag *cf_tag = array_back(&scanner->cf_tags);
    if (cf_tag->type != CF_QUERY) {
        return false;
    }

    lexer->mark_end(lexer);

    size_t tag_len = cf_tag->tag_name.size;
    if (tag_len > MAX_CF_END_DELIMITER_SIZE - 5) return false;

    char end_delimiter[MAX_CF_END_DELIMITER_SIZE];
    memcpy(end_delimiter, "</CF", 4);
    memcpy(&end_delimiter[4], cf_tag->tag_name.contents, tag_len);
    end_delimiter[4 + tag_len] = '\0';

    size_t delimiter_index = 0;
    size_t end_delim_len = 4 + tag_len;

    while (lexer->lookahead) {
        if (towupper(lexer->lookahead) == end_delimiter[delimiter_index]) {
            delimiter_index++;
            if (delimiter_index == end_delim_len) {
                break;
            }
            advance(lexer);
        } else {
            delimiter_index = 0;
            advance(lexer);
            lexer->mark_end(lexer);
        }
    }

    lexer->result_symbol = CF_QUERY_CONTENT;
    return true;
}

static bool scan_cfxml_content(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {

    if (scanner->cf_tags.size == 0) {
        return false;
    }

    Tag *cf_tag = array_back(&scanner->cf_tags);
    if (cf_tag->type != CF_XML) {
        return false;
    }

    lexer->mark_end(lexer);

    size_t tag_len = cf_tag->tag_name.size;
    if (tag_len > MAX_CF_END_DELIMITER_SIZE - 5) return false;

    char end_delimiter[MAX_CF_END_DELIMITER_SIZE];
    memcpy(end_delimiter, "</CF", 4);
    memcpy(&end_delimiter[4], cf_tag->tag_name.contents, tag_len);
    end_delimiter[4 + tag_len] = '\0';

    size_t delimiter_index = 0;
    size_t end_delim_len = 4 + tag_len;

    while (lexer->lookahead) {
        if (towupper(lexer->lookahead) == end_delimiter[delimiter_index]) {
            delimiter_index++;
            if (delimiter_index == end_delim_len) {
                break;
            }
            advance(lexer);
        } else {
            delimiter_index = 0;
            advance(lexer);
            lexer->mark_end(lexer);
        }
    }

    lexer->result_symbol = CF_XML_CONTENT;
    return true;
}


static bool scan_cfscript_content(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {

    if (scanner->cf_tags.size == 0) {
        return false;
    }

    Tag *cf_tag = array_back(&scanner->cf_tags);
    if (cf_tag->type != CF_SCRIPT) {
        return false;
    }

    lexer->mark_end(lexer);

    size_t tag_len = cf_tag->tag_name.size;
    if (tag_len > MAX_CF_END_DELIMITER_SIZE - 5) return false;

    char end_delimiter[MAX_CF_END_DELIMITER_SIZE];
    memcpy(end_delimiter, "</CF", 4);
    memcpy(&end_delimiter[4], cf_tag->tag_name.contents, tag_len);
    end_delimiter[4 + tag_len] = '\0';

    size_t delimiter_index = 0;
    size_t end_delim_len = 4 + tag_len;

    while (lexer->lookahead) {
        if (towupper(lexer->lookahead) == end_delimiter[delimiter_index]) {
            delimiter_index++;
            if (delimiter_index == end_delim_len) {
                break;
            }
            advance(lexer);
        } else {
            delimiter_index = 0;
            advance(lexer);
            lexer->mark_end(lexer);
        }
    }

    lexer->result_symbol = CF_SCRIPT_CONTENT;
    return true;
}

static bool scan_cfsavecontent_body_type(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols, unsigned count, bool is_cfquery_context) {
    if (scanner->cf_tags.size == 0) return false;
    Tag *cf_tag = array_back(&scanner->cf_tags);
    if (cf_tag->type != CF_SAVECONTENT) return false;

    // Default to cfml
    unsigned result = CF_SAVECONTENT_BODY_CFML;

    // Mark end at current position - this is a zero-width token
    lexer->mark_end(lexer);

    // Peek ahead for <!--- @content TYPE --->
    // Skip whitespace first
    while (iswspace(lexer->lookahead)) advance(lexer);

    if (lexer->lookahead == '<') {
        advance(lexer);
        if (lexer->lookahead == '!') {
            advance(lexer);
            if (lexer->lookahead == '-') {
                advance(lexer);
                if (lexer->lookahead == '-') {
                    advance(lexer);
                    if (lexer->lookahead == '-') {
                        advance(lexer);
                        // Skip whitespace after <!---
                        while (iswspace(lexer->lookahead)) advance(lexer);
                        // Check for @content
                        const char *directive = "@content";
                        size_t di = 0;
                        bool matched = true;
                        while (di < 8) {
                            if (lexer->lookahead != directive[di]) { matched = false; break; }
                            advance(lexer);
                            di++;
                        }
                        if (matched) {
                            // Skip whitespace
                            while (iswspace(lexer->lookahead)) advance(lexer);
                            // Read type word
                            char type_buf[16];
                            int len = 0;
                            while (iswalpha(lexer->lookahead) && len < 15) {
                                type_buf[len++] = towlower(lexer->lookahead);
                                advance(lexer);
                            }
                            type_buf[len] = '\0';
                            if (strcmp(type_buf, "script") == 0) result = CF_SAVECONTENT_BODY_SCRIPT;
                            else if (strcmp(type_buf, "css") == 0) result = CF_SAVECONTENT_BODY_CSS;
                            else if (strcmp(type_buf, "xml") == 0) result = CF_SAVECONTENT_BODY_XML;
                            else if (strcmp(type_buf, "sql") == 0) result = CF_SAVECONTENT_BODY_SQL;
                            else if (strcmp(type_buf, "raw") == 0) result = CF_SAVECONTENT_BODY_RAW;
                            else if (strcmp(type_buf, "html") == 0) result = CF_SAVECONTENT_BODY_HTML;
                        }
                    }
                }
            }
        }
    }

    if (!VS(valid_symbols, result, count)) return false;
    lexer->result_symbol = result;
    return true;
}

static bool scan_cfsavecontent_content(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {
    if (scanner->cf_tags.size == 0) return false;
    Tag *cf_tag = array_back(&scanner->cf_tags);
    if (cf_tag->type != CF_SAVECONTENT) return false;

    lexer->mark_end(lexer);

    size_t tag_len = cf_tag->tag_name.size;
    if (tag_len > MAX_CF_END_DELIMITER_SIZE - 5) return false;

    char end_delimiter[MAX_CF_END_DELIMITER_SIZE];
    memcpy(end_delimiter, "</CF", 4);
    memcpy(&end_delimiter[4], cf_tag->tag_name.contents, tag_len);
    end_delimiter[4 + tag_len] = '\0';
    size_t end_delim_len = 4 + tag_len;

    bool has_content = false;

    while (lexer->lookahead) {
        if (lexer->lookahead == '<') {
            lexer->mark_end(lexer);
            advance(lexer);
            if (lexer->lookahead == '/') {
                advance(lexer);
                size_t i = 2;
                bool matched = true;
                while (i < end_delim_len) {
                    if (towupper(lexer->lookahead) != end_delimiter[i]) { matched = false; break; }
                    i++;
                    if (i < end_delim_len) advance(lexer);
                }
                if (matched) {
                    lexer->result_symbol = CF_SAVECONTENT_CONTENT;
                    return has_content;
                }
            }
            has_content = true;
            lexer->mark_end(lexer);
        } else {
            has_content = true;
            advance(lexer);
            lexer->mark_end(lexer);
        }
    }

    lexer->result_symbol = CF_SAVECONTENT_CONTENT;
    return has_content;
}

static bool scan_raw_text(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {
    if (scanner->tags.size == 0) {
        return false;
    }

    lexer->mark_end(lexer);

    const char *end_delimiter = array_back(&scanner->tags)->type == SCRIPT ? "</SCRIPT" : "</STYLE";

    bool stop_at_cfml = !is_cfquery_context &&
        (array_back(&scanner->tags)->type == SCRIPT || array_back(&scanner->tags)->type == STYLE);

    bool has_content = false;
    unsigned delimiter_index = 0;
    while (lexer->lookahead) {
        // CFML boundary checks (only when not mid-delimiter match)
        if (stop_at_cfml && delimiter_index == 0) {
            if (lexer->lookahead == '#' && scanner->cfoutput_depth > 0) {
                break;
            }
            if (lexer->lookahead == '<') {
                advance(lexer);
                if (towupper(lexer->lookahead) == 'C') {
                    advance(lexer);
                    if (towupper(lexer->lookahead) == 'F') {
                        // Stop before <cf
                        break;
                    }
                    lexer->mark_end(lexer);
                    has_content = true;
                    continue;
                } else if (lexer->lookahead == '/') {
                    advance(lexer);
                    if (towupper(lexer->lookahead) == 'C') {
                        advance(lexer);
                        if (towupper(lexer->lookahead) == 'F') {
                            // Stop before </cf
                            break;
                        }
                        lexer->mark_end(lexer);
                        has_content = true;
                        continue;
                    } else if (towupper(lexer->lookahead) == end_delimiter[2]) {
                        // Potential </script or </style
                        delimiter_index = 3;
                        advance(lexer);
                        continue;
                    }
                    lexer->mark_end(lexer);
                    has_content = true;
                    continue;
                } else {
                    lexer->mark_end(lexer);
                    has_content = true;
                    continue;
                }
            }
        }

        if (towupper(lexer->lookahead) == end_delimiter[delimiter_index]) {
            delimiter_index++;
            if (delimiter_index == strlen(end_delimiter)) {
                break;
            }
            advance(lexer);
        } else {
            delimiter_index = 0;
            advance(lexer);
            lexer->mark_end(lexer);
            has_content = true;
        }
    }

    if (!has_content) {
        return false;
    }

    lexer->result_symbol = RAW_TEXT;
    return true;
}

static void pop_tag(Scanner *scanner, bool is_cf_context) {
    if ( is_cf_context ) {
        Tag popped_tag = array_pop(&scanner->cf_tags);
        tag_free(&popped_tag);
    } else {
        Tag popped_tag = array_pop(&scanner->tags);
        tag_free(&popped_tag);
    }
}

static bool scan_implicit_end_tag(Scanner *scanner, TSLexer *lexer, bool is_cf_context, bool is_cfquery_context, bool from_tag_open) {

    Tag *parent = is_cf_context
        ? (scanner->cf_tags.size == 0 ? NULL : array_back(&scanner->cf_tags))
        : (scanner->tags.size == 0 ? NULL : array_back(&scanner->tags));

    bool is_closing_tag = false;
    if (lexer->lookahead == '/') {
        is_closing_tag = true;
        advance(lexer);
    } else {
        // Void tag processing
        if (!is_cf_context && parent && tag_is_void(parent)) {
            pop_tag(scanner, false);
            lexer->result_symbol = IMPLICIT_END_TAG;
            return true;
        }
        // Void tag processing
        if (is_cf_context && parent && cf_tag_is_void(parent)) {
            pop_tag(scanner, true);
            lexer->result_symbol = IMPLICIT_CF_END_TAG;
            return true;
        }
    }

    TagNameResult result = scan_tag_name(lexer, is_cfquery_context);
    if (result.tag_name.size == 0 && !lexer->eof(lexer)) {
        array_delete(&result.tag_name);
        return false;
    }

    if (result.is_cf_tag && !is_closing_tag &&
        ((result.tag_name.size == 4 && memcmp(result.tag_name.contents, "ELSE", 4) == 0) ||
         (result.tag_name.size == 6 && memcmp(result.tag_name.contents, "ELSEIF", 6) == 0))) {
        array_delete(&result.tag_name);
        if (is_cf_context && parent &&
            parent->type != CF_IF && parent->type != CF_ELSEIF && parent->type != CF_ELSE) {
            pop_tag(scanner, true);
            lexer->result_symbol = IMPLICIT_CF_END_TAG;
            return true;
        }
        if (!is_cf_context && scanner->tags.size > 0) {
            // Check if there's a CF_IF on the cf_tags stack whose html_depth
            // is less than current tags.size (meaning HTML tags opened inside cfif)
            for (unsigned i = scanner->cf_tags.size; i > 0; i--) {
                Tag *ct = &scanner->cf_tags.contents[i - 1];
                if (ct->type == CF_IF || ct->type == CF_ELSEIF || ct->type == CF_ELSE) {
                    if (scanner->tags.size > ct->html_depth) {
                        pop_tag(scanner, false);
                        lexer->result_symbol = IMPLICIT_END_TAG;
                        return true;
                    }
                    break;
                }
            }
        }
        return false;
    }

    if (result.is_cf_tag && !is_cf_context && is_closing_tag) {
        // A CF closing tag in HTML context: only implicitly close HTML tags
        // that were opened inside this CF tag (i.e. tags.size > cf html_depth)
        Tag cf_next = cf_tag_for_name(result.tag_name);
        unsigned cf_html_depth = 0;
        bool found = false;
        for (unsigned i = scanner->cf_tags.size; i > 0; i--) {
            if (tag_eq(&scanner->cf_tags.contents[i - 1], &cf_next)) {
                cf_html_depth = scanner->cf_tags.contents[i - 1].html_depth;
                found = true;
                break;
            }
        }
        tag_free(&cf_next);
        if (found && scanner->tags.size > cf_html_depth) {
            pop_tag(scanner, false);
            lexer->result_symbol = IMPLICIT_END_TAG;
            return true;
        }
        return false;
    }

    if (result.is_cf_tag && !is_cf_context) {
        array_delete(&result.tag_name);
        return false;
    }


    Tag next_tag = is_cf_context ? cf_tag_for_name(result.tag_name) : tag_for_name(result.tag_name);

    if (is_closing_tag) {
        // The tag correctly closes the topmost element on the stack
        if (is_cf_context ? (scanner->cf_tags.size > 0 && tag_eq(array_back(&scanner->cf_tags), &next_tag))
                          : (scanner->tags.size > 0 && tag_eq(array_back(&scanner->tags), &next_tag))) {
            // Before accepting the CF close, check if HTML tags opened inside
            // this CF tag need implicit closing (e.g. <cfloop><span></cfloop>)
            if (is_cf_context && scanner->tags.size > array_back(&scanner->cf_tags)->html_depth) {
                pop_tag(scanner, false);
                lexer->result_symbol = IMPLICIT_END_TAG;
                tag_free(&next_tag);
                return true;
            }
            tag_free(&next_tag);
            return false;
        }

        // if (is_cf ? (scanner->cf_tags.size > 0 && !tag_eq(array_back(&scanner->cf_tags), &next_tag))
        //                   : (scanner->tags.size > 0 && !tag_eq(array_back(&scanner->tags), &next_tag))) {
        //     pop_tag(scanner, is_cf);
        //     lexer->result_symbol = is_cf ? IMPLICIT_CF_END_TAG : IMPLICIT_END_TAG;
        //     tag_free(&next_tag);
        //     return true;
        // }

        // Otherwise, dig deeper and queue implicit end tags (to be nice in
        // the case of malformed HTML)
        if (is_cf_context) {
            for (unsigned i = scanner->cf_tags.size; i > 0; i--) {
                if (tag_eq(&scanner->cf_tags.contents[i - 1], &next_tag)) {
                    pop_tag(scanner, true);
                    lexer->result_symbol = IMPLICIT_CF_END_TAG;
                    tag_free(&next_tag);
                    return true;
                }
            }
        } else {
            for (unsigned i = scanner->tags.size; i > 0; i--) {
                if (tag_eq(&scanner->tags.contents[i - 1], &next_tag)) {
                    pop_tag(scanner, false);
                    lexer->result_symbol = IMPLICIT_END_TAG;
                    tag_free(&next_tag);
                    return true;
                }
            }
        }
    } else {

        if (from_tag_open && is_cf_context && !result.is_cf_tag && parent && tag_eq(parent, &next_tag)) {
            pop_tag(scanner, true);
            lexer->result_symbol = IMPLICIT_CF_END_TAG;
            tag_free(&next_tag);
            return true;
        }

        if (!is_cf_context && parent && tag_eq(parent, &next_tag) && tag_implicitly_closes_self(parent)) {
            pop_tag(scanner, false);
            lexer->result_symbol = IMPLICIT_END_TAG;
            tag_free(&next_tag);
            return true;
        }

        if (
            parent &&
            (
                (lexer->eof(lexer))
                || (is_cf_context && lexer->eof(lexer))
            )
        ) {
            pop_tag(scanner, is_cf_context);
            lexer->result_symbol = is_cf_context ? IMPLICIT_CF_END_TAG : IMPLICIT_END_TAG;
            tag_free(&next_tag);
            return true;
        }
    }

    tag_free(&next_tag);
    return false;
}

static bool scan_start_tag_name(Scanner *scanner, TSLexer *lexer, bool is_cf_context, bool is_cfquery_context) {

    // Dynamic tag name: <#expression#>
    if (lexer->lookahead == '#') {
        Tag tag = tag_new();
        tag.type = DYNAMIC;
        array_push(&scanner->tags, tag);
        lexer->result_symbol = START_TAG_NAME;
        return true;
    }

    TagNameResult result = scan_tag_name(lexer, is_cfquery_context);

    if (result.tag_name.size == 0) {
        array_delete(&result.tag_name);
        return false;
    }

    if ( result.is_cf_tag && !is_cf_context ) {
        array_delete(&result.tag_name);
        return false;
    }

    // bool is_cf = result.is_cf_tag || is_cf_context;
    Tag tag = is_cf_context ? cf_tag_for_name(result.tag_name) : tag_for_name(result.tag_name);

    // printf("scan_start_tag_name: tag=%.*s, is_cf_tag=%d, is_cf_context=%d, type=%d\n",
    // (int)result.tag_name.size, result.tag_name.contents, result.is_cf_tag, is_cf_context, tag.type);

    switch (tag.type) {
        case SCRIPT:
            lexer->result_symbol = SCRIPT_START_TAG_NAME;
            break;
        case STYLE:
            lexer->result_symbol = STYLE_START_TAG_NAME;
            break;
        case CF_VOID:
            if (is_cf_context && tag.tag_name.size == 9 &&
                memcmp(tag.tag_name.contents, "COMPONENT", 9) == 0) {
                scanner->cfcomponent_depth++;
                lexer->result_symbol = CF_COMPONENT_START_TAG_NAME;
            } else {
                lexer->result_symbol = CF_VOID_START_TAG_NAME;
            }
            tag_free(&tag);
            return true;
        case CF_SET:
            lexer->result_symbol = CF_SET_START_TAG_NAME;
            tag_free(&tag); /* not pushed to the stack — free its owned tag_name */
            return true;
        case CF_RETURN:
            lexer->result_symbol = CF_RETURN_START_TAG_NAME;
            tag_free(&tag); /* not pushed to the stack — free its owned tag_name */
            return true;
        case CF_IF:
            lexer->result_symbol = CF_IF_START_TAG_NAME;
            break;
        case CF_ELSEIF:
            lexer->result_symbol = CF_ELSEIF_TAG_NAME;
            tag_free(&tag); /* not pushed to the stack — free its owned tag_name */
            return true;
        case CF_ELSE:
            lexer->result_symbol = CF_ELSE_TAG_NAME;
            tag_free(&tag); /* not pushed to the stack — free its owned tag_name */
            return true;
        case CF_XML:
            lexer->result_symbol = CF_XML_START_TAG_NAME;
            break;
        case CF_QUERY:
            lexer->result_symbol = CF_QUERY_START_TAG_NAME;
            break;
        case CF_SCRIPT:
            lexer->result_symbol = CF_SCRIPT_START_TAG_NAME;
            break;
         case CF_SAVECONTENT:
            lexer->result_symbol = CF_SAVECONTENT_START_TAG_NAME;
            break;
        case CF_OUTPUT:
            lexer->result_symbol = CF_OUTPUT_START_TAG_NAME;
            if (is_cf_context) {
                scanner->cfoutput_depth++;
            }
            break;
        case CF_FUNCTION:
            lexer->result_symbol = CF_FUNCTION_START_TAG_NAME;
            if (is_cf_context) {
                scanner->cffunction_depth++;
            }
            break;
        default:
            lexer->result_symbol = is_cf_context ? CF_START_TAG_NAME : START_TAG_NAME;
            break;
    }

    if ( is_cf_context ) {
        tag.html_depth = scanner->tags.size;
        array_push(&scanner->cf_tags, tag);
    } else {
        array_push(&scanner->tags, tag);
    }

    return true;
}

static void set_end_tag_symbol(Scanner *scanner, TSLexer *lexer, Tag *tag, bool is_cf_context, bool is_cfquery_context) {
    if (is_cf_context && tag->type == CF_OUTPUT) {
        if (scanner->cfoutput_depth > 0) scanner->cfoutput_depth--;
        lexer->result_symbol = CF_END_TAG_NAME;
    } else if (is_cf_context && tag->type == CF_FUNCTION) {
        if (scanner->cffunction_depth > 0) scanner->cffunction_depth--;
        lexer->result_symbol = CF_FUNCTION_END_TAG_NAME;
    } else if (is_cf_context && tag->type == CF_XML) {
        lexer->result_symbol = CF_XML_END_TAG_NAME;
    } else if (is_cf_context && tag->type == CF_QUERY) {
        lexer->result_symbol = CF_QUERY_END_TAG_NAME;
    } else if (is_cf_context && tag->type == CF_SCRIPT) {
        lexer->result_symbol = CF_SCRIPT_END_TAG_NAME;
    } else if (is_cf_context && tag->type == CF_SAVECONTENT) {
        lexer->result_symbol = CF_SAVECONTENT_END_TAG_NAME;
    } else if (is_cf_context && tag->type == CF_IF) {
        lexer->result_symbol = CF_IF_END_TAG_NAME;
    } else {
        lexer->result_symbol = is_cf_context ? CF_END_TAG_NAME : END_TAG_NAME;
    }
}

static bool scan_end_tag_name(Scanner *scanner, TSLexer *lexer, bool is_cf_context, bool is_cfquery_context) {

    // Dynamic closing tag: </#expression#>
    if (lexer->lookahead == '#') {
        if (scanner->tags.size > 0 && array_back(&scanner->tags)->type == DYNAMIC) {
            pop_tag(scanner, false);
        }
        lexer->result_symbol = END_TAG_NAME;
        return true;
    }

    TagNameResult result = scan_tag_name(lexer, is_cfquery_context);

    if (result.tag_name.size == 0) {
        array_delete(&result.tag_name);
        return false;
    }

    if ( result.is_cf_tag && !is_cf_context ) {
        array_delete(&result.tag_name);
        return false;
    }

    // printf("scan_end_tag_name: tag=%.*s, is_cf_context=%d, tags.size=%d, cf_tags.size=%d\n",
    // (int)result.tag_name.size, result.tag_name.contents, is_cf_context,
    // scanner->tags.size, scanner->cf_tags.size);

    // bool is_cf = result.is_cf_tag || is_cf_context;

    Tag tag = is_cf_context ? cf_tag_for_name(result.tag_name) : tag_for_name(result.tag_name);

    // cfcomponent is a void tag (never pushed); just decrement depth
    if (is_cf_context && tag.type == CF_VOID &&
        tag.tag_name.size == 9 && memcmp(tag.tag_name.contents, "COMPONENT", 9) == 0) {
        if (scanner->cfcomponent_depth > 0) scanner->cfcomponent_depth--;
        lexer->result_symbol = CF_COMPONENT_END_TAG_NAME;
        tag_free(&tag);
        return true;
    }

    // Determine the minimum HTML stack index we can search to.
    // CF control-flow tags record html_depth when pushed; don't search below that.
    unsigned html_floor = 0;
    if (!is_cf_context && scanner->cf_tags.size > 0) {
        html_floor = array_back(&scanner->cf_tags)->html_depth;
    }

    Tag *tag_back = (is_cf_context) ? ( scanner->cf_tags.size > 0 ? array_back(&scanner->cf_tags) : NULL )
                   : ( scanner->tags.size > html_floor) ? array_back(&scanner->tags) : NULL;

    if ( tag_back && tag_eq(tag_back, &tag) ) {
        pop_tag(scanner, is_cf_context);
        set_end_tag_symbol(scanner, lexer, &tag, is_cf_context, is_cfquery_context);
    } else {
        // Search deeper in the stack for a matching tag
        bool found = false;
        if (is_cf_context) {
            for (unsigned i = scanner->cf_tags.size; i > 0; i--) {
                if (tag_eq(&scanner->cf_tags.contents[i - 1], &tag)) {
                    found = true;
                    break;
                }
            }
        } else {
            for (unsigned i = scanner->tags.size; i > html_floor; i--) {
                if (tag_eq(&scanner->tags.contents[i - 1], &tag)) {
                    found = true;
                    break;
                }
            }
        }

        if (found) {
            pop_tag(scanner, is_cf_context);
            set_end_tag_symbol(scanner, lexer, &tag, is_cf_context, is_cfquery_context);
        } else {
            lexer->result_symbol = is_cf_context ? ERRONEOUS_CF_END_TAG_NAME : ERRONEOUS_END_TAG_NAME;
        }
    }

    tag_free(&tag);
    return true;
}

static bool scan_cf_self_closing_tag_delimiter(Scanner *scanner, TSLexer *lexer, bool is_void, bool is_cfquery_context) {
     if (lexer->lookahead == '>') {
        advance(lexer);
        if (is_void) {
            lexer->result_symbol = CF_SELF_CLOSING_VOID_TAG_DELIMITER;
        } else {
            lexer->result_symbol = CF_SELF_CLOSING_TAG_DELIMITER;
            if (scanner->cf_tags.size > 0) {
                pop_tag(scanner, true);
            }

        }
        return true;
    }
    return false;
}

static bool scan_self_closing_tag_delimiter(Scanner *scanner, TSLexer *lexer, bool is_cfquery_context) {
     if (lexer->lookahead == '>') {
        advance(lexer);
        if (scanner->tags.size > 0) {
            pop_tag(scanner, false);
            lexer->result_symbol = SELF_CLOSING_TAG_DELIMITER;
        }
        return true;
    }
    return false;
}

// Check if the current position matches a CFML word operator (case-insensitive).
static bool scan_cfml_word_operator(TSLexer *lexer) {
    char buf[11] = {0};
    int len = 0;
    for (; len < 10 && iswalpha(lexer->lookahead); len++) {
        buf[len] = towlower(lexer->lookahead);
        skip(lexer);
    }
    bool at_end = !iswalnum(lexer->lookahead);
    if (!at_end) return false;

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

static bool scan_automatic_semicolon(TSLexer *lexer, bool comment_condition, bool *scanned_comment, bool is_cfquery_context) {
    lexer->result_symbol = AUTOMATIC_SEMICOLON;
    lexer->mark_end(lexer);

    for (;;) {
        if (lexer->lookahead == 0) {
            return true;
        }

        if (lexer->lookahead == '/') {
            WhitespaceResult result = scan_whitespace_and_comments(lexer, scanned_comment, false, is_cfquery_context);
            if (result == false) {
                return false;
            }
            if (result == true && comment_condition && lexer->lookahead != ',' && lexer->lookahead != '=') {
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

    if (scan_whitespace_and_comments(lexer, scanned_comment, true, is_cfquery_context) == REJECT) {
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
        case 'i':
        case 'a': case 'A':
        case 'o': case 'O':
        case 'e': case 'E':
        case 'n': case 'N':
        case 'g': case 'G':
        case 'l': case 'L':
        case 'm': case 'M':
            return !scan_cfml_word_operator(lexer);

        default:
            break;
    }

    return true;
}

static bool scan_ternary_qmark(TSLexer *lexer, bool is_cfquery_context) {
    for (;;) {
        if (!iswspace(lexer->lookahead)) {
            break;
        }
        skip(lexer);
    }

    if (lexer->lookahead == '?') {
        advance(lexer);

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

static bool scan_closetag_delim(Scanner *scanner, TSLexer *lexer, bool is_cf_context, bool is_cfquery_context) {
    if ( lexer->lookahead == '>' ) {
        advance(lexer);
        lexer->mark_end(lexer);
        lexer->result_symbol = is_cf_context ? CLOSE_CF_TAG_DELIM : CLOSE_TAG_DELIM;
        return true;
    } else {
        return false;
    }
}

static bool scanner_in_hash_eval_context(Scanner *scanner, bool is_cfquery_context) {
    if (scanner->cfoutput_depth > 0 || scanner->cfcomponent_depth > 0 || scanner->cffunction_depth > 0) {
        return true;
    }
    return false;
}

static bool scan_cf_component_content(TSLexer *lexer, bool is_cfquery_context) {
    // Skip whitespace and script-style comments (// and /* */)
    for (;;) {
        while (iswspace(lexer->lookahead)) advance(lexer);
        if (lexer->lookahead == '/') {
            advance(lexer);
            if (lexer->lookahead == '/') {
                advance(lexer);
                while (lexer->lookahead != 0 && lexer->lookahead != '\n') advance(lexer);
            } else if (lexer->lookahead == '*') {
                advance(lexer);
                while (lexer->lookahead != 0) {
                    if (lexer->lookahead == '*') { advance(lexer); if (lexer->lookahead == '/') { advance(lexer); break; } }
                    else advance(lexer);
                }
            } else {
                return false;
            }
        } else {
            break;
        }
    }

    // Read the first word
    char word[16];
    int len = 0;
    while (iswalpha(lexer->lookahead) && len < 15) {
        word[len++] = towlower(lexer->lookahead);
        advance(lexer);
    }
    word[len] = '\0';

    // Must not be followed by another identifier char (e.g. 'componentFoo')
    if (iswalnum(lexer->lookahead) || lexer->lookahead == '_') return false;

    // If the first word is a modifier, skip whitespace and read the next word
    if (strcmp(word, "abstract") == 0 || strcmp(word, "static") == 0) {
        while (iswspace(lexer->lookahead)) advance(lexer);
        len = 0;
        while (iswalpha(lexer->lookahead) && len < 15) {
            word[len++] = towlower(lexer->lookahead);
            advance(lexer);
        }
        word[len] = '\0';
        if (iswalnum(lexer->lookahead) || lexer->lookahead == '_') return false;
    }

    if (strcmp(word, "component") != 0 && strcmp(word, "property") != 0 &&
            strcmp(word, "interface") != 0 && strcmp(word, "import") != 0) {
        return false;
    }

    // Consume the rest of the file
    while (lexer->lookahead != 0) advance(lexer);
    lexer->mark_end(lexer);
    lexer->result_symbol = CF_COMPONENT_CONTENT;
    return true;
}

static bool external_scanner_scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols, unsigned count, bool is_cfquery_context) {

    if (!VS(valid_symbols, HTML_TEXT, count) && !VS(valid_symbols, RAW_TEXT, count)) {
        while (iswspace(lexer->lookahead)) {
            skip(lexer);
        }
    }

    if ((VS(valid_symbols, START_HASH_EXPRESSION, count) || VS(valid_symbols, SINGLE_HASH, count) || VS(valid_symbols, HASH_EMPTY, count))
            && !VS(valid_symbols, AUTOMATIC_SEMICOLON, count) && lexer->lookahead == '#') {
        advance(lexer);
        if (lexer->lookahead == '#') {
            advance(lexer);
            lexer->mark_end(lexer);
            lexer->result_symbol = HASH_EMPTY;
        } else if (scanner_in_hash_eval_context(scanner, is_cfquery_context)) {
            lexer->result_symbol = START_HASH_EXPRESSION;
        } else {
            lexer->mark_end(lexer);
            lexer->result_symbol = SINGLE_HASH;
        }
        return true;
    }

    if (VS(valid_symbols, CF_COMPONENT_CONTENT, count)
            && scanner->tags.size == 0 && scanner->cf_tags.size == 0
            && scan_cf_component_content(lexer, is_cfquery_context)) {
        return true;
    }

    if (VS(valid_symbols, RAW_TEXT, count) && !VS(valid_symbols, START_TAG_NAME, count) && !VS(valid_symbols, END_TAG_NAME, count)) {
        if (scan_raw_text(scanner, lexer, is_cfquery_context)) {
            return true;
        }
    }

    if (VS(valid_symbols, CF_XML_CONTENT, count)) {
        return scan_cfxml_content(scanner, lexer, is_cfquery_context);
    }

    if (VS(valid_symbols, CF_QUERY_CONTENT, count)) {
        return scan_cfquery_content(scanner, lexer, is_cfquery_context);
    }

    if (VS(valid_symbols, CF_SCRIPT_CONTENT, count)) {
        return scan_cfscript_content(scanner, lexer, is_cfquery_context);
    }

    if (VS(valid_symbols, CF_SAVECONTENT_BODY_CFML, count) || VS(valid_symbols, CF_SAVECONTENT_BODY_HTML, count) ||
        VS(valid_symbols, CF_SAVECONTENT_BODY_SCRIPT, count) ||
        VS(valid_symbols, CF_SAVECONTENT_BODY_CSS, count) || VS(valid_symbols, CF_SAVECONTENT_BODY_XML, count) ||
        VS(valid_symbols, CF_SAVECONTENT_BODY_SQL, count) || VS(valid_symbols, CF_SAVECONTENT_BODY_RAW, count)) {
        if (scan_cfsavecontent_body_type(scanner, lexer, valid_symbols, count, is_cfquery_context)) {
            return true;
        }
    }

    if (VS(valid_symbols, CF_SAVECONTENT_CONTENT, count)) {
        return scan_cfsavecontent_content(scanner, lexer, is_cfquery_context);
    }


    if (VS(valid_symbols, HTML_TEXT, count) && scan_html_text(scanner, lexer, is_cfquery_context)) {
        return true;
    }

    switch (lexer->lookahead) {
        case ';':
            return false;
        break;
        case '<':
            lexer->mark_end(lexer);
            advance(lexer);

            if (VS(valid_symbols, CFML_COMMENT, count) && lexer->lookahead == '!') {
                advance(lexer);
                return scan_comment(lexer, is_cfquery_context);
            }

            if (implicit_cf_end_tag_valid(valid_symbols, count)) {
                return scan_implicit_end_tag(scanner, lexer, true, is_cfquery_context, true);
            }

            if (VS(valid_symbols, IMPLICIT_END_TAG, count)) {
                return scan_implicit_end_tag(scanner, lexer, false, is_cfquery_context, true);
            }

            break;

        case '\0':

            if (implicit_cf_end_tag_valid(valid_symbols, count)) {
                return scan_implicit_end_tag(scanner, lexer, true, is_cfquery_context, true);
            }

            if (VS(valid_symbols, IMPLICIT_END_TAG, count)) {
                return scan_implicit_end_tag(scanner, lexer, false, is_cfquery_context, true);
            }
            break;

        case '/':

            advance(lexer);
            if (lexer->lookahead == '>') {
                if (VS(valid_symbols, CF_SELF_CLOSING_TAG_DELIMITER, count)) {
                    return scan_cf_self_closing_tag_delimiter(scanner, lexer, false, is_cfquery_context);
                }
                if (VS(valid_symbols, CF_SELF_CLOSING_VOID_TAG_DELIMITER, count)) {
                    return scan_cf_self_closing_tag_delimiter(scanner, lexer, true, is_cfquery_context);
                }
                if (VS(valid_symbols, SELF_CLOSING_TAG_DELIMITER, count)) {
                    return scan_self_closing_tag_delimiter(scanner, lexer, is_cfquery_context);
                }
                if (VS(valid_symbols, CLOSE_CF_TAG_DELIM, count)) {
                    return scan_closetag_delim(scanner, lexer, true, is_cfquery_context);
                }
                if (VS(valid_symbols, CLOSE_TAG_DELIM, count)) {
                    return scan_closetag_delim(scanner, lexer, false, is_cfquery_context);
                }
            } else if (lexer->lookahead == '/' || lexer->lookahead == '*') {
                if (!scan_script_comment(lexer, is_cfquery_context)) {
                    return false;
                }
            }

            break;

        default:
            if (valid_start_tag_name(valid_symbols, count) && no_content_symbols(valid_symbols, count)) {
                return scan_start_tag_name(scanner, lexer, valid_cf_start_tag_name(valid_symbols, count), is_cfquery_context);
            }
            if (valid_end_tag_name(valid_symbols, count) && no_content_symbols(valid_symbols, count)) {
                return scan_end_tag_name(scanner, lexer, valid_cf_end_tag_name(valid_symbols, count), is_cfquery_context);
            }

            if (VS(valid_symbols, IMPLICIT_END_TAG, count)) {
                return scan_implicit_end_tag(scanner, lexer, false, is_cfquery_context, false);
            } else if (VS(valid_symbols, IMPLICIT_CF_END_TAG, count)) {
                return scan_implicit_end_tag(scanner, lexer, true, is_cfquery_context, false);
            }

            if (VS(valid_symbols, ERRONEOUS_END_TAG_NAME, count)) {
                return scan_end_tag_name(scanner, lexer, false, is_cfquery_context);
            } else if (VS(valid_symbols, ERRONEOUS_CF_END_TAG_NAME, count)) {
                return scan_end_tag_name(scanner, lexer, true, is_cfquery_context);
            }

            if (VS(valid_symbols, CF_COMPONENT_END_TAG_NAME, count)) {
                return scan_end_tag_name(scanner, lexer, true, is_cfquery_context);
            }

            if (VS(valid_symbols, CLOSE_CF_TAG_DELIM, count)) {
                if (scan_closetag_delim(scanner, lexer, true, is_cfquery_context)) {
                    return true;
                }
            }

            if (VS(valid_symbols, CLOSE_TAG_DELIM, count)) {
                if (scan_closetag_delim(scanner, lexer, false, is_cfquery_context)) {
                    return true;
                }
            }
    }

    if (VS(valid_symbols, AUTOMATIC_SEMICOLON, count)) {
        bool scanned_comment = false;
        bool ret = scan_automatic_semicolon(lexer, !VS(valid_symbols, LOGICAL_OR, count), &scanned_comment, is_cfquery_context);
        if (!ret && !scanned_comment && VS(valid_symbols, TERNARY_QMARK, count) && lexer->lookahead == '?') {
            return scan_ternary_qmark(lexer, is_cfquery_context);
        }
        return ret;
    }

    if (VS(valid_symbols, TERNARY_QMARK, count) || VS(valid_symbols, ELVIS_OPERATOR, count)) {
        return scan_ternary_qmark(lexer, is_cfquery_context);
    }

    return false;
}
