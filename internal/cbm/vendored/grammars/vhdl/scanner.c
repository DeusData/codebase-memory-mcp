// #define DEBUG
//------------------------------------------------------------------------------

#include <stddef.h>
#include <stdio.h>
#include <string.h>
//------------------------------------------------------------------------------

#include "tree_sitter/parser.h"
#include "debug_macros.h"
//------------------------------------------------------------------------------

#include "TokenType.h"
#include "TokenTree.h"
//------------------------------------------------------------------------------

typedef struct ScannerTag{
    bool      is_in_directive;
    bool      is_block_comment; // else line comment
    TokenType bit_string_base;
    int       base;
} Scanner;
//------------------------------------------------------------------------------

#include "TokenType.inc"
#include "TokenTree.inc"
//------------------------------------------------------------------------------

void *tree_sitter_vhdl_external_scanner_create(void)
{
    Scanner* scanner = ts_calloc(1, sizeof(Scanner));
    return scanner;
}
//------------------------------------------------------------------------------

void tree_sitter_vhdl_external_scanner_destroy(void *payload)
{
    Scanner *scanner = (Scanner *)payload;
    if(scanner) ts_free(scanner);
}
//------------------------------------------------------------------------------

unsigned tree_sitter_vhdl_external_scanner_serialize(void *payload, char *buffer)
{
    Scanner *scanner = (Scanner *)payload;
    memcpy(buffer, scanner, sizeof(Scanner));
    return sizeof(Scanner);
}
//------------------------------------------------------------------------------

void tree_sitter_vhdl_external_scanner_deserialize(void *payload, const char *buffer, unsigned length)
{
    Scanner *scanner = (Scanner *)payload;
    if(length > 0 && length <= sizeof(Scanner) && buffer)
        memcpy(scanner, buffer, length);
}
//------------------------------------------------------------------------------

static bool is_whitespace(int32_t character)
{
    return character == ' '  || character == '\t' ||
           character == '\n' || character == '\r';
}
//------------------------------------------------------------------------------

static void skip_whitespace(TSLexer* lexer, bool skip_newline, bool discard)
{
    if(skip_newline){
        while(is_whitespace(lexer->lookahead)){
            debug("Skipping whitespace and newlines");
            lexer->advance(lexer, discard);
        }
    }else{
        while(lexer->lookahead == ' ' || lexer->lookahead == '\t'){
            debug("Skipping whitespace");
            lexer->advance(lexer, discard);
        }
    }
}
//------------------------------------------------------------------------------

static bool bounded_token(TSLexer* lexer, int32_t bound)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == bound){
            lexer->advance(lexer, false);
            lexer->mark_end(lexer);
            if(lexer->lookahead != bound) return true;
        }
        if(lexer->lookahead == '\r') return false;
        if(lexer->lookahead == '\n') return false;
        lexer->advance(lexer, false);
    }
    return false;
}
//------------------------------------------------------------------------------

static bool is_letter_or_digit(int32_t lookahead)
{
    return (lookahead >= 'a' && lookahead <= 'z') ||
           (lookahead >= '0' && lookahead <= '9');
}
//------------------------------------------------------------------------------

static bool finish_identifier(TSLexer* lexer, bool expect_letter)
{
    int32_t lookahead = lowercase(lexer->lookahead);
    bool    result = false;

    if(expect_letter){
        if(!is_letter_or_digit(lookahead)) return false;
    }

    while(!lexer->eof(lexer)){
        lexer->mark_end(lexer);
        if(lookahead == '_') lookahead = advance(lexer);
        if(!is_letter_or_digit(lookahead)) return result;
        lookahead = advance(lexer);
        result = true;
    }
    return result;
}
//------------------------------------------------------------------------------

static bool is_special_value(int32_t value)
{
    switch(value){
        case 'u':
        case 'x':
        case 'z':
        case 'w':
        case 'l':
        case 'h':
        case '-':
            return true;
        default:
            return false;
    }
}
//------------------------------------------------------------------------------

static bool binary_string_literal(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '_') lexer->advance(lexer, false);
        if((lexer->lookahead < '0' || lexer->lookahead > '1') &&
           !is_special_value(lowercase(lexer->lookahead))) break;
        lexer->advance(lexer, false);
    }
    if(lexer->lookahead != '"') return false;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
}
//------------------------------------------------------------------------------

static bool octal_string_literal(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '_') lexer->advance(lexer, false);
        if((lexer->lookahead < '0' || lexer->lookahead > '7') &&
           !is_special_value(lowercase(lexer->lookahead))) break;
        lexer->advance(lexer, false);
    }
    if(lexer->lookahead != '"') return false;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
}
//------------------------------------------------------------------------------

static bool decimal_string_literal(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '_') lexer->advance(lexer, false);
        if((lexer->lookahead < '0' || lexer->lookahead > '9') &&
           !is_special_value(lowercase(lexer->lookahead))) break;
        lexer->advance(lexer, false);
    }
    if(lexer->lookahead != '"') return false;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
}
//------------------------------------------------------------------------------

static bool is_hex_digit(int32_t character)
{
    return (character >= '0' && character <= '9') ||
           (character >= 'a' && character <= 'f') ||
           (character >= 'A' && character <= 'F');
}
//------------------------------------------------------------------------------

static bool hex_string_literal(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '_') lexer->advance(lexer, false);
        if(!is_hex_digit(lexer->lookahead) &&
           !is_special_value(lowercase(lexer->lookahead))) break;
        lexer->advance(lexer, false);
    }
    if(lexer->lookahead != '"') return false;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    return true;
}
//------------------------------------------------------------------------------

static bool finish_string_literal(TSLexer* lexer, TokenType type)
{
    switch(type){
        case BASE_SPECIFIER_BINARY:  return binary_string_literal (lexer);
        case BASE_SPECIFIER_OCTAL:   return octal_string_literal  (lexer);
        case BASE_SPECIFIER_DECIMAL: return decimal_string_literal(lexer);
        case BASE_SPECIFIER_HEX:     return hex_string_literal    (lexer);
        default:
            error("Unrecognised type %s", token_type_to_string(type));
            return false;
    }
}
//------------------------------------------------------------------------------

static void finish_line_comment(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '\r' || lexer->lookahead == '\n'){
            lexer->advance(lexer, false);
            lexer->mark_end(lexer);
            return;
        }
        lexer->advance(lexer, false);
    }
    lexer->mark_end(lexer);
}
//------------------------------------------------------------------------------

static void finish_block_comment(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '*'){
            lexer->advance(lexer, false);
            if(lexer->lookahead == '/') return;
            lexer->mark_end(lexer);

        }else if(is_whitespace(lexer->lookahead)){
            lexer->advance(lexer, false);

        }else{
            lexer->advance(lexer, false);
            lexer->mark_end(lexer);
        }
    }
}
//------------------------------------------------------------------------------

static bool finish_block_comment_end(TSLexer* lexer)
{
    while(!lexer->eof(lexer)){
        if(lexer->lookahead == '*'){
            lexer->advance(lexer, false);
            if(lexer->lookahead == '/'){
                lexer->advance(lexer, false);
                lexer->mark_end(lexer);
                return true;
            }
        }else{
            lexer->advance(lexer, false);
        }
    }
    return false;
}
//------------------------------------------------------------------------------

static void finish_comment_content(TSLexer* lexer, bool is_block_comment)
{
    lexer->mark_end(lexer);
    if(is_block_comment) finish_block_comment(lexer);
    else                 finish_line_comment(lexer);
}
//------------------------------------------------------------------------------

static bool may_start_with_digit(const bool* valid_symbols)
{
    return valid_symbols[TOKEN_DECIMAL_INTEGER] ||
           valid_symbols[TOKEN_DECIMAL_FLOAT]   ||
           valid_symbols[TOKEN_BASED_BASE]      ||
           valid_symbols[TOKEN_BIT_STRING_LENGTH];
}
//------------------------------------------------------------------------------

static int parse_integer(TSLexer* lexer)
{
    int result = 0;
    while(!lexer->eof(lexer)){
        lexer->mark_end(lexer);
        if(lexer->lookahead == '_') lexer->advance(lexer, false);
        if(lexer->lookahead < '0' || lexer->lookahead > '9') return result;

        result *= 10;
        result += lexer->lookahead - '0';
        lexer->advance(lexer, false);
    }
    return result;
}
//------------------------------------------------------------------------------

static bool parse_decimal_exponent(TSLexer* lexer)
{
    lexer->advance(lexer, false);
    if(lexer->lookahead == '+' || lexer->lookahead == '-') lexer->advance(lexer, false);
    if(lexer->lookahead < '0' || lexer->lookahead > '9') return false;

    parse_integer(lexer);

    return true;
}
//------------------------------------------------------------------------------

static bool parse_decimal_fraction(TSLexer* lexer)
{
    lexer->advance(lexer, false);
    if(lexer->lookahead < '0' || lexer->lookahead > '9') return false;

    parse_integer(lexer);

    if(lexer->lookahead == 'e' || lexer->lookahead == 'E'){
        return parse_decimal_exponent(lexer);
    }
    return true;
}
//------------------------------------------------------------------------------

static int to_digit(int32_t character)
{
    if(character >= '0' && character <= '9') return character - '0';
    if(character >= 'a' && character <= 'z') return character - 'a' + 10;
    if(character >= 'A' && character <= 'Z') return character - 'A' + 10;
    return -1;
}
//------------------------------------------------------------------------------

static bool based_integer(TSLexer* lexer, int base)
{
    while(!lexer->eof(lexer)){
        lexer->mark_end(lexer);
        if(lexer->lookahead == '_') lexer->advance(lexer, false);
        int digit = to_digit(lexer->lookahead);
        if(digit < 0) return true;
        if(digit >= base) return false;
        lexer->advance(lexer, false);
    }
    return true;
}
//------------------------------------------------------------------------------

static bool parse_base_literal(TSLexer* lexer, int base)
{
    lexer->advance(lexer, false);
    lexer->result_symbol = TOKEN_BASED_INTEGER;

    if(!based_integer(lexer, base)) return false;
    if(lexer->lookahead == '.'){
        lexer->advance(lexer, false);
        lexer->result_symbol = TOKEN_BASED_FLOAT;
        if(!based_integer(lexer, base)) return false;
    }
    if(lexer->lookahead != '#') return false;
    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    if(lexer->lookahead == 'e' || lexer->lookahead == 'E'){
        return parse_decimal_exponent(lexer);
    }
    return true;
}
//------------------------------------------------------------------------------

static bool parse_digit_based_literal(Scanner* scanner, TSLexer* lexer)
{
    lexer->result_symbol = TOKEN_DECIMAL_INTEGER;

    scanner->base = parse_integer(lexer);
    debug("base = %d", scanner->base);
    // NOTE: VHDL-2008 limits the base to 16, but I feel it's pointless to
    //       enforce, so I don't.  A future version will most likely relax
    //       that rule.

    switch(lowercase(lexer->lookahead)){
        case '.':
            lexer->result_symbol = TOKEN_DECIMAL_FLOAT;
            return parse_decimal_fraction(lexer);

        case 'e':
            return parse_decimal_exponent(lexer);

        case '#':
            lexer->result_symbol = TOKEN_BASED_BASE;
            return true;

        case 'b': case 'o': case 'd': case 'x':
            lexer->advance(lexer, false);
            if(lexer->lookahead != '"') return true;
            lexer->result_symbol = TOKEN_BIT_STRING_LENGTH;
            return true;

        case 'u': case 's':
            switch(advance(lexer)){
                case 'b': case 'o': case 'x': break;
                default: return true;
            }
            lexer->advance(lexer, false);
            if(lexer->lookahead != '"') return true;
            lexer->result_symbol = TOKEN_BIT_STRING_LENGTH;
            return true;

        default:
            return true;
    }
}
//------------------------------------------------------------------------------

static bool graphic_characters(TSLexer* lexer)
{
    if(lexer->lookahead == '\n' || lexer->lookahead == '\r') return false;

    while(!lexer->eof(lexer)){
        debug("lookahead = %c(0x%x)", (char)lexer->lookahead, lexer->lookahead);
        if(lexer->lookahead == ' '  || lexer->lookahead == '\t' ||
           lexer->lookahead == '\n' || lexer->lookahead == '\r'){
            return true;
        }
        lexer->advance(lexer, false);
    }
    return false;
}
//------------------------------------------------------------------------------

bool tree_sitter_vhdl_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols)
{
    Scanner *scanner = (Scanner *)payload;
    #ifdef DEBUG
        if(valid_symbols[ERROR_SENTINEL]){
            debug("Error correction mode");
        }else{
            debug("Looking for:");
            for(int n = 0; n < ERROR_SENTINEL; n++){
                if(valid_symbols[n]) printf("    %s\n", token_type_to_string(n));
            }
            printf("\n");
        }
    #endif

    if(!valid_symbols[ERROR_SENTINEL] && valid_symbols[TOKEN_COMMENT_CONTENT]){
        finish_comment_content(lexer, scanner->is_block_comment);
        lexer->result_symbol = TOKEN_COMMENT_CONTENT;
        debug("Returning type TOKEN_COMMENT_CONTENT");
        return true;

    }else if(!valid_symbols[ERROR_SENTINEL] && valid_symbols[TOKEN_BLOCK_COMMENT_END]){
        if(finish_block_comment_end(lexer)){
            lexer->result_symbol = TOKEN_BLOCK_COMMENT_END;
            debug("Returning type TOKEN_BLOCK_COMMENT_END");
            return true;
        }
        debug("Returning false");
        return false;
    }

    skip_whitespace(lexer, !scanner->is_in_directive, true);

    if(valid_symbols[END_OF_FILE] && lexer->eof(lexer)){
        lexer->result_symbol = END_OF_FILE;
        debug("Returning type END_OF_FILE");
        return true;

    }else if(valid_symbols[IDENTIFIER] && lexer->lookahead == '\\'){
        lexer->advance(lexer, false);
        if(!bounded_token(lexer, '\\')) return false;
        lexer->result_symbol = IDENTIFIER;
        debug("Returning type IDENTIFIER");
        return true;

    }else if((valid_symbols[TOKEN_CHARACTER_LITERAL] ||
              valid_symbols[LIBRARY_CONSTANT_STD_LOGIC]) && lexer->lookahead == '\''){
        int32_t lookahead = advance(lexer);
        lexer->result_symbol = TOKEN_CHARACTER_LITERAL;
        if(lookahead == '0' || lookahead == '1' || is_special_value(lookahead)){
            if(valid_symbols[LIBRARY_CONSTANT_STD_LOGIC]){
                lexer->result_symbol = LIBRARY_CONSTANT_STD_LOGIC;
            }
        }
        if(lexer->eof(lexer)) return false;
        lexer->advance(lexer, false);
        if(lexer->lookahead != '\'') return false;
        lexer->advance(lexer, false);
        debug("Returning type %s", token_type_to_string(lexer->result_symbol));
        return true;

    }else if(lexer->lookahead >= '0' && lexer->lookahead <= '9'){
        if(!may_start_with_digit(valid_symbols)) return false;
        if(!parse_digit_based_literal(scanner, lexer)) return false;
        debug("returning type %s", token_type_to_string(lexer->result_symbol));
        return true;

    }else if(!valid_symbols[ERROR_SENTINEL] && valid_symbols[TOKEN_BIT_STRING_VALUE]){
        if(lexer->lookahead == '"'){
            lexer->advance(lexer, false);
            lexer->result_symbol = TOKEN_BIT_STRING_VALUE;
            if(finish_string_literal(lexer, scanner->bit_string_base)){
                debug("Returning type TOKEN_BIT_STRING_VALUE");
                return true;
            }
        }
        debug("Returning false");
        return false;

    }else if(!valid_symbols[ERROR_SENTINEL] &&
             (valid_symbols[TOKEN_BASED_INTEGER] || valid_symbols[TOKEN_BASED_FLOAT])){
        if(lexer->lookahead == '#'){
            if(parse_base_literal(lexer, scanner->base)){
                debug("Returning type %s", token_type_to_string(lexer->result_symbol));
                return true;
            }
        }
        debug("Returning false");
        return false;

    }else if(!valid_symbols[ERROR_SENTINEL] && valid_symbols[DIRECTIVE_BODY] && graphic_characters(lexer)){
        lexer->result_symbol = DIRECTIVE_BODY;
        debug("returning type DIRECTIVE_BODY");
        return true;
    }

    bool first_char_is_letter = (lexer->lookahead >= 'a' && lexer->lookahead <= 'z') ||
                                (lexer->lookahead >= 'A' && lexer->lookahead <= 'Z');

    bool first_char_is_double_quote = (lexer->lookahead == '"');

    const TokenType* types = token_tree_match(lexer);

    if(!types && first_char_is_letter){
        /* This works because all registered tokens in the search tree that
         * start with a letter are also valid identifiers.  If the tree search
         * exits early, whatever came before is a valid identifier
         *
         * The underscore corner cases are handled by IDENTIFIER_EXPECTING_LETTER
         */
        lexer->mark_end(lexer);
        finish_identifier(lexer, false);
        lexer->result_symbol = IDENTIFIER;
        debug("Returning type IDENTIFIER");
        return valid_symbols[IDENTIFIER];
    }

    if(!types && first_char_is_double_quote){
        if(!bounded_token(lexer, '"')) return false;
        lexer->result_symbol = TOKEN_STRING_LITERAL;
        debug("Returning type TOKEN_STRING_LITERAL");
        return valid_symbols[TOKEN_STRING_LITERAL];
    }

    bool found_can_be_identifier    = false;
    bool found_cannot_be_identifier = false;

    if(types){
        while(*types){
            if(*types == LINE_COMMENT_START){
                if(valid_symbols[TOKEN_LINE_COMMENT_START]){
                    scanner->is_block_comment = false;
                    skip_whitespace(lexer, false, false);
                    lexer->mark_end(lexer);
                    lexer->result_symbol = TOKEN_LINE_COMMENT_START;
                    debug("Returning type TOKEN_LINE_COMMENT_START");
                    return true;
                }
                debug("Returning false");
                return false;

            }else if(*types == BLOCK_COMMENT_START){
                if(valid_symbols[TOKEN_BLOCK_COMMENT_START]){
                    scanner->is_block_comment = true;
                    skip_whitespace(lexer, true, false);
                    lexer->mark_end(lexer);
                    lexer->result_symbol = TOKEN_BLOCK_COMMENT_START;
                    debug("Returning type TOKEN_BLOCK_COMMENT_START");
                    return true;
                }
                debug("Returning false");
                return false;

            }else if(can_start_identifier(*types) &&
                     finish_identifier(lexer, *types == IDENTIFIER_EXPECTING_LETTER)){
                lexer->result_symbol = IDENTIFIER;
                debug("Returning type IDENTIFIER");
                return true;

            }else if(is_base_specifier(*types)){
                if(lexer->lookahead == '"'){
                    scanner->bit_string_base = *types;
                    lexer->result_symbol = TOKEN_BIT_STRING_BASE;
                    debug("Returning type TOKEN_BIT_STRING_BASE");
                    return true;
                }else if(!types[1]){
                    finish_identifier(lexer, false);
                    lexer->result_symbol = IDENTIFIER;
                    debug("Returning type IDENTIFIER");
                    return true;
                }

            }else if(*types == LIBRARY_CONSTANT_CHARACTER && lexer->lookahead == '"'){
                // This could actually be a bit-string base, so let the while continue
                // It is not possible to follow a character constant with a string

            }else if(*types == TOKEN_OPERATOR_SYMBOL ||
                     *types == TOKEN_STRING_LITERAL_STD_LOGIC){
                if(lexer->lookahead == '"'){
                    if(valid_symbols[TOKEN_STRING_LITERAL]){
                        lexer->advance(lexer, false);
                        if(!bounded_token(lexer, '"')) return false;
                        lexer->result_symbol = TOKEN_STRING_LITERAL;
                        debug("Returning type TOKEN_STRING_LITERAL");
                        return true;
                    }else{
                        debug("Returning false");
                        return false;
                    }
                }else if(valid_symbols[TOKEN_STRING_LITERAL_STD_LOGIC] && contains(types, TOKEN_STRING_LITERAL_STD_LOGIC)){
                    lexer->result_symbol = TOKEN_STRING_LITERAL_STD_LOGIC;
                    debug("Returning type TOKEN_STRING_LITERAL_STD_LOGIC");
                    return true;

                }else if(valid_symbols[*types]){
                    lexer->result_symbol = *types;
                    debug("Returning type %s", token_type_to_string(*types));
                    return true;
                }else if(!types[1] && valid_symbols[TOKEN_STRING_LITERAL]){
                    lexer->result_symbol = TOKEN_STRING_LITERAL;
                    debug("Returning type TOKEN_STRING_LITERAL");
                    return true;
                }

            }else if(*types == STRING_LITERAL_STD_LOGIC_START){
                if(valid_symbols[TOKEN_STRING_LITERAL_STD_LOGIC] && binary_string_literal(lexer)){
                    if(lexer->lookahead != '"'){
                        lexer->result_symbol = TOKEN_STRING_LITERAL_STD_LOGIC;
                        debug("Returning type TOKEN_STRING_LITERAL_STD_LOGIC");
                        return valid_symbols[TOKEN_STRING_LITERAL_STD_LOGIC];
                    }else{
                        lexer->advance(lexer, false);
                        // and drop down to continue the string parsing below
                    }
                }
                if(valid_symbols[TOKEN_STRING_LITERAL] && bounded_token(lexer, '"')){
                    lexer->result_symbol = TOKEN_STRING_LITERAL;
                    debug("Returning type TOKEN_STRING_LITERAL");
                    return valid_symbols[TOKEN_STRING_LITERAL];
                }
                debug("Returning false");
                return false;

            }else if(*types < ERROR_SENTINEL && valid_symbols[*types]){
                lexer->result_symbol = *types;

                if(scanner->is_in_directive){
                   scanner->is_in_directive = (*types != DIRECTIVE_NEWLINE);
                }else{
                   scanner->is_in_directive = (*types == DELIMITER_GRAVE_ACCENT);
                }

                if(valid_symbols[DIRECTIVE_CONSTANT_BUILTIN] && contains(types, DIRECTIVE_CONSTANT_BUILTIN))
                    lexer->result_symbol = DIRECTIVE_CONSTANT_BUILTIN;

                debug("Returning type %s", token_type_to_string(lexer->result_symbol));
                return true;

            }else if(can_be_identifier(scanner, *types)){
                found_can_be_identifier = true;
            }else{
                found_cannot_be_identifier = true;
            }
            types++;
        }
    }
    if(valid_symbols[IDENTIFIER] && found_can_be_identifier && !found_cannot_be_identifier){
        lexer->result_symbol = IDENTIFIER;
        debug("Returning type IDENTIFIER");
        return true;
    }

    debug("Returning false...");
    return false;
}
//------------------------------------------------------------------------------

