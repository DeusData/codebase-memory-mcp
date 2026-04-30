#ifndef TREE_SITTER_RST_PARSER_H_
#define TREE_SITTER_RST_PARSER_H_

#include "scanner.h"
#include "tokens.h"

#define IM_EMPHASIS 1u << 0u
#define IM_STRONG 1u << 1u
#define IM_INTERPRETED_TEXT 1u << 2u
#define IM_INTERPRETED_TEXT_PREFIX 1u << 3u
#define IM_LITERAL 1u << 4u
#define IM_SUBSTITUTION_REFERENCE 1u << 5u
#define IM_INLINE_TARGET 1u << 6u
#define IM_FOOTNOTE_REFERENCE 1u << 7u
#define IM_CITATION_REFERENCE 1u << 8u
#define IM_REFERENCE 1u << 9u

static bool parse_indent(RSTScanner* scanner);
static bool parse_overline(RSTScanner* scanner);
static bool parse_underline(RSTScanner* scanner);
static bool fallback_adornment(RSTScanner* scanner, int32_t adornment, int adornment_length);

static bool parse_char_bullet(RSTScanner* scanner);
static bool parse_numeric_bullet(RSTScanner* scanner);
static bool parse_inner_numeric_bullet(RSTScanner* scanner, bool parenthesized);
static bool parse_explict_markup_start(RSTScanner* scanner);
static bool parse_inner_list_element(RSTScanner* scanner, int consumed_chars, enum TokenType token_type);
static bool parse_field_mark(RSTScanner* scanner);
static bool parse_field_mark_end(RSTScanner* scanner);
static bool parse_inner_field_mark(RSTScanner* scanner);

static bool parse_literal_block_mark(RSTScanner* scanner);
static bool parse_innner_literal_block_mark(RSTScanner* scanner);
static bool parse_quoted_literal_block(RSTScanner* scanner);

static bool parse_line_block_mark(RSTScanner* scanner);

static bool parse_attribution_mark(RSTScanner* scanner);

static bool parse_doctest_block_mark(RSTScanner* scanner);

static bool parse_label(RSTScanner* scanner);
static int parse_inner_label_name(RSTScanner* scanner);
static bool parse_inner_alphanumeric_label(RSTScanner* scanner);
static bool parse_target_name(RSTScanner* scanner);
static bool parse_anonymous_target_mark(RSTScanner* scanner);
static bool parse_directive_mark(RSTScanner* scanner);
static bool parse_substitution_mark(RSTScanner* scanner);

static bool parse_inline_markup(RSTScanner* scanner);
static bool parse_inner_inline_markup(RSTScanner* scanner, unsigned type);
static bool parse_reference(RSTScanner* scanner);
static bool parse_inner_reference(RSTScanner* scanner);
static bool parse_standalone_hyperlink(RSTScanner* scanner);
static bool parse_inner_standalone_hyperlink(RSTScanner* scanner);
static bool parse_role(RSTScanner* scanner);
static bool parse_role_name(RSTScanner* scanner);
static bool parse_inner_role(RSTScanner* scanner);
static bool parse_text(RSTScanner* scanner, bool mark_end);

#endif /* TREE_SITTER_RST_PARSER_H_ */
