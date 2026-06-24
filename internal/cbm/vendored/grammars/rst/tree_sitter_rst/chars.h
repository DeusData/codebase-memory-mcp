#ifndef TREE_SITTER_RST_CHARS_H_
#define TREE_SITTER_RST_CHARS_H_

#include <stdbool.h>
#include "../tree_sitter/parser.h"

#include "scanner.h"

#define CHAR_EOF 0
#define CHAR_NEWLINE 10
#define CHAR_CARRIAGE_RETURN 13
#define CHAR_NBSP 160

#define CHAR_SPACE ' '
#define CHAR_FORM_FEED '\f'
#define CHAR_TAB '\t'
#define CHAR_VERTICAL_TAB '\v'

// number of spaces to expand a tab to
#define TAB_STOP 8

static bool is_newline(int32_t c);
static bool is_space(int32_t c);
static bool is_number(int32_t c);
static bool is_abc_lower(int32_t c);
static bool is_abc_upper(int32_t c);
static bool is_abc(int32_t c);
static bool is_alphanumeric(int32_t c);

static bool is_adornment_char(int32_t c);

static bool is_start_char(int32_t c);
static bool is_end_char(int32_t c);
static bool is_inline_markup_start_char(int32_t c);
static bool is_inline_markup_end_char(int32_t c);

static bool is_internal_reference_char(int32_t c);

static bool is_char_bullet(int32_t c);
static bool is_numeric_bullet(int32_t c);
static bool is_numeric_bullet_simple(int32_t c);
static bool is_numeric_bullet_roman_lower(int32_t c);
static bool is_numeric_bullet_roman_upper(int32_t c);
static bool is_numeric_bullet_abc_lower(int32_t c);
static bool is_numeric_bullet_abc_upper(int32_t c);
static bool is_known_schema(char* string, unsigned string_len);
static bool is_invalid_uri_char(int32_t c);

#define CHAR_EMDASH 8212

static bool is_attribution_mark(int32_t c);

static int get_indent_level(RSTScanner* scanner);

#endif /* ifndef TREE_SITTER_RST_CHARS_H_ */
