#include "parser.h"

#include "chars.h"
#include "tokens.h"
#include <stdio.h>

static bool parse_indent(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  lexer->mark_end(lexer);

  int indent = 0;
  int newlines = 0;
  while (true) {
    if (scanner->lookahead == CHAR_SPACE
        || scanner->lookahead == CHAR_VERTICAL_TAB
        || scanner->lookahead == CHAR_FORM_FEED
        || scanner->lookahead == CHAR_NBSP) {
      indent += 1;
    } else if (scanner->lookahead == CHAR_TAB) {
      indent += TAB_STOP;
    } else if (scanner->lookahead == CHAR_EOF) {
      indent = 0;
      newlines++;
      break;
    } else if (scanner->lookahead == CHAR_CARRIAGE_RETURN) {
      indent = 0;
    } else if (scanner->lookahead == CHAR_NEWLINE) {
      newlines++;
      indent = 0;
    } else {
      break;
    }
    scanner->skip(scanner);
  }

  int current_indent = scanner->back(scanner);
  if (indent > current_indent && valid_symbols[T_INDENT]) {
    scanner->push(scanner, indent);
    lexer->result_symbol = T_INDENT;
    return true;
  }
  if (newlines) {
    if (indent < current_indent && valid_symbols[T_DEDENT]) {
      scanner->pop(scanner);
      lexer->result_symbol = T_DEDENT;
      return true;
    }

    if ((newlines > 1 || scanner->lookahead == CHAR_EOF) && valid_symbols[T_BLANKLINE]) {
      lexer->result_symbol = T_BLANKLINE;
      return true;
    }

    if (newlines == 1 && valid_symbols[T_NEWLINE_INDENT] && indent > current_indent) {
      scanner->push(scanner, indent);
      lexer->result_symbol = T_NEWLINE_INDENT;
      return true;
    }

    if (valid_symbols[T_NEWLINE]) {
      lexer->result_symbol = T_NEWLINE;
      return true;
    }
  }
  return false;
}

static bool parse_overline(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;
  int32_t adornment = scanner->lookahead;

  if (!is_adornment_char(scanner->lookahead)
      || (!valid_symbols[T_OVERLINE] && !valid_symbols[T_TRANSITION])) {
    return false;
  }

  scanner->advance(scanner);
  lexer->mark_end(lexer);
  int overline_length = 1;

  while (true) {
    if (scanner->lookahead != adornment) {
      bool ok = fallback_adornment(scanner, adornment, overline_length);
      if (ok) {
        return true;
      }
      if (is_space(scanner->lookahead)) {
        break;
      }
      return parse_text(scanner, false);
    }
    scanner->advance(scanner);
    overline_length++;
  }

  // Mark the transition token
  lexer->mark_end(lexer);

  // Consume all whitespaces.
  while (is_space(scanner->lookahead) && !is_newline(scanner->lookahead)) {
    scanner->advance(scanner);
  }

  if (!is_newline(scanner->lookahead)) {
    return parse_text(scanner, false);
  }

  scanner->advance(scanner);

  bool is_empty = true;
  while (!is_newline(scanner->lookahead)) {
    if (!is_space(scanner->lookahead)) {
      is_empty = false;
    }
    scanner->advance(scanner);
  }

  if (is_empty) {
    // Transitions need to be at least 4 chars long
    if (overline_length >= 4 && valid_symbols[T_TRANSITION]) {
      lexer->result_symbol = T_TRANSITION;
      return true;
    }
    return parse_text(scanner, false);
  }

  scanner->advance(scanner);

  int underline_length = 0;

  while (!is_newline(scanner->lookahead)) {
    if (scanner->lookahead != adornment) {
      if (is_space(scanner->lookahead)) {
        break;
      }
      return parse_text(scanner, false);
    }
    scanner->advance(scanner);
    underline_length++;
  }

  // Consume all whitespaces.
  while (is_space(scanner->lookahead) && !is_newline(scanner->lookahead)) {
    scanner->advance(scanner);
  }

  if (!is_newline(scanner->lookahead)) {
    return parse_text(scanner, false);
  }

  // The overline_length and the underline_length length must match
  if (overline_length >= 1 && overline_length == underline_length) {
    lexer->result_symbol = T_OVERLINE;
    return true;
  }

  return parse_text(scanner, false);
}

static bool parse_underline(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;
  int32_t adornment = scanner->lookahead;

  if (!is_adornment_char(scanner->lookahead)
      || (!valid_symbols[T_UNDERLINE] && !valid_symbols[T_TRANSITION])) {
    return false;
  }

  scanner->advance(scanner);
  lexer->mark_end(lexer);
  int underline_length = 1;

  while (!is_newline(scanner->lookahead)) {
    if (scanner->lookahead != adornment) {
      bool ok = fallback_adornment(scanner, adornment, underline_length);
      if (ok) {
        return true;
      }
      if (is_space(scanner->lookahead)) {
        break;
      }
      return parse_text(scanner, false);
    }
    scanner->advance(scanner);
    underline_length++;
  }

  // Mark the transition token
  lexer->mark_end(lexer);

  // Consume all whitespaces.
  while (is_space(scanner->lookahead) && !is_newline(scanner->lookahead)) {
    scanner->advance(scanner);
  }

  if (!is_newline(scanner->lookahead)) {
    return parse_text(scanner, false);
  }

  // Transitions need to be at least 4 chars long
  if (underline_length >= 4 && valid_symbols[T_TRANSITION]) {
    lexer->result_symbol = T_TRANSITION;
    return true;
  }

  if (underline_length >= 1 && valid_symbols[T_UNDERLINE]) {
    lexer->result_symbol = T_UNDERLINE;
    return true;
  }

  return parse_text(scanner, false);
}

/// If the adnorment is not valid, try to parse a different token.
///
/// Lots of adornments are also valid tokens, so we need to check for each one of them.
static bool fallback_adornment(RSTScanner* scanner, int32_t adornment, int adornment_length)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (adornment_length == 1) {
    if (is_space(scanner->lookahead)) {
      if (is_char_bullet(adornment) && valid_symbols[T_CHAR_BULLET]) {
        bool ok = parse_inner_list_element(scanner, 1, T_CHAR_BULLET);
        if (ok) {
          return true;
        }
      } else if (adornment == '|' && valid_symbols[T_LINE_BLOCK_MARK]) {
        bool ok = parse_inner_list_element(scanner, 1, T_LINE_BLOCK_MARK);
        if (ok) {
          return true;
        }
      }
    } else {
      if (adornment == '*' && valid_symbols[T_EMPHASIS]) {
        return parse_inner_inline_markup(scanner, IM_EMPHASIS);
      }
      if (adornment == ':' && (valid_symbols[T_ROLE_NAME_PREFIX] || valid_symbols[T_ROLE_NAME_SUFFIX])) {
        bool ok = parse_inner_role(scanner);
        if (ok) {
          return true;
        }
        return parse_text(scanner, false);
      }
      if (adornment == ':' && valid_symbols[T_FIELD_MARK]) {
        bool ok = parse_inner_field_mark(scanner);
        if (ok) {
          return true;
        }
        return parse_text(scanner, false);
      }
      if (adornment == '`' && (valid_symbols[T_INTERPRETED_TEXT] || valid_symbols[T_INTERPRETED_TEXT_PREFIX] || valid_symbols[T_REFERENCE])) {
        return parse_inner_inline_markup(scanner, IM_INTERPRETED_TEXT | IM_INTERPRETED_TEXT_PREFIX | IM_REFERENCE);
      }
      if (adornment == '|' && valid_symbols[T_SUBSTITUTION_REFERENCE]) {
        return parse_inner_inline_markup(scanner, IM_SUBSTITUTION_REFERENCE);
      }
      if (adornment == '_' && scanner->lookahead == '`' && valid_symbols[T_INLINE_TARGET]) {
        return parse_inner_inline_markup(scanner, IM_INLINE_TARGET);
      }
      if (adornment == '[' && (valid_symbols[T_FOOTNOTE_REFERENCE] || valid_symbols[T_CITATION_REFERENCE])) {
        return parse_inner_inline_markup(scanner, IM_FOOTNOTE_REFERENCE | IM_CITATION_REFERENCE);
      }
      if (adornment == '#'
          && (scanner->lookahead == '.' || scanner->lookahead == ')')) {
        scanner->advance(scanner);
        bool ok = parse_inner_list_element(scanner, 2, T_NUMERIC_BULLET);
        if (ok) {
          return true;
        }
      }
      if (adornment == '(' && is_numeric_bullet(scanner->lookahead) && valid_symbols[T_NUMERIC_BULLET]) {
        return parse_inner_numeric_bullet(scanner, true);
      }
    }
  } else if (adornment_length >= 2) {
    if (is_space(scanner->lookahead)) {
      if (adornment_length == 3
          && adornment == '>'
          && valid_symbols[T_DOCTEST_BLOCK_MARK]) {
        lexer->mark_end(lexer);
        lexer->result_symbol = T_DOCTEST_BLOCK_MARK;
        return true;
      }
      if (adornment_length == 2 && adornment == '.') {
        return parse_inner_list_element(scanner, 2, T_EXPLICIT_MARKUP_START);
      }
      if (adornment_length == 2
          && adornment == '_'
          && valid_symbols[T_ANONYMOUS_TARGET_MARK]) {
        lexer->mark_end(lexer);
        lexer->result_symbol = T_ANONYMOUS_TARGET_MARK;
        return true;
      }
      if (adornment_length == 2
          && adornment == ':'
          && (valid_symbols[T_LITERAL_INDENTED_BLOCK_MARK] || valid_symbols[T_LITERAL_QUOTED_BLOCK_MARK])) {
        return parse_innner_literal_block_mark(scanner);
      }
    } else {
      if (adornment == '*' && valid_symbols[T_STRONG]) {
        return parse_inner_inline_markup(scanner, IM_STRONG);
      }
      if (adornment == '`' && valid_symbols[T_LITERAL]) {
        return parse_inner_inline_markup(scanner, IM_LITERAL);
      }
      if (adornment == '|' && valid_symbols[T_SUBSTITUTION_REFERENCE]) {
        return parse_inner_inline_markup(scanner, IM_SUBSTITUTION_REFERENCE);
      }
      if (adornment == '[' && (valid_symbols[T_FOOTNOTE_REFERENCE] || valid_symbols[T_CITATION_REFERENCE])) {
        return parse_inner_inline_markup(scanner, IM_FOOTNOTE_REFERENCE | IM_CITATION_REFERENCE);
      }
    }
  }
  return false;
}

bool parse_char_bullet(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (!is_char_bullet(scanner->lookahead) || !valid_symbols[T_CHAR_BULLET]) {
    return false;
  }

  scanner->advance(scanner);
  bool ok = parse_inner_list_element(scanner, 1, T_CHAR_BULLET);
  if (ok) {
    return true;
  }

  return parse_text(scanner, true);
}

static bool parse_numeric_bullet(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (!valid_symbols[T_NUMERIC_BULLET]) {
    return false;
  }

  bool parenthesized = false;
  if (scanner->lookahead == '(') {
    scanner->advance(scanner);
    parenthesized = true;
  }

  if (is_numeric_bullet(scanner->lookahead)) {
    return parse_inner_numeric_bullet(scanner, parenthesized);
  }

  return false;
}

static bool parse_inner_numeric_bullet(RSTScanner* scanner, bool parenthesized)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!is_numeric_bullet(scanner->lookahead) || !valid_symbols[T_NUMERIC_BULLET]) {
    return false;
  }

  scanner->advance(scanner);
  int consumed_chars = 1;

  if (is_numeric_bullet_simple(scanner->previous)) {
    while (is_numeric_bullet_simple(scanner->lookahead) && scanner->lookahead != '#') {
      scanner->advance(scanner);
      consumed_chars++;
    }
  } else if (is_numeric_bullet_abc_lower(scanner->previous)) {
    if (is_numeric_bullet_roman_lower(scanner->previous)) {
      while (is_numeric_bullet_roman_lower(scanner->lookahead)) {
        scanner->advance(scanner);
        consumed_chars++;
      }
    }
  } else if (is_numeric_bullet_abc_upper(scanner->previous)) {
    if (is_numeric_bullet_roman_upper(scanner->previous)) {
      while (is_numeric_bullet_roman_upper(scanner->lookahead)) {
        scanner->advance(scanner);
        consumed_chars++;
      }
    }
  } else {
    return false;
  }

  if ((parenthesized && scanner->lookahead == ')')
      || (!parenthesized && (scanner->lookahead == '.' || scanner->lookahead == ')'))) {
    scanner->advance(scanner);
    consumed_chars++;
    consumed_chars += parenthesized ? 1 : 0;
    bool ok = parse_inner_list_element(scanner, consumed_chars, T_NUMERIC_BULLET);
    if (ok) {
      return true;
    }
  } else {
    if (is_abc(scanner->lookahead) && valid_symbols[T_STANDALONE_HYPERLINK]) {
      return parse_inner_standalone_hyperlink(scanner);
    }
    if (is_alphanumeric(scanner->lookahead) && valid_symbols[T_REFERENCE]) {
      return parse_reference(scanner);
    }
    if (valid_symbols[T_TEXT]) {
      lexer->mark_end(lexer);
      lexer->result_symbol = T_TEXT;
      return true;
    }
    return false;
  }
  return parse_text(scanner, true);
}

static bool parse_explict_markup_start(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (scanner->lookahead != '.' || !valid_symbols[T_EXPLICIT_MARKUP_START]) {
    return false;
  }

  scanner->advance(scanner);

  if (scanner->lookahead != '.') {
    return false;
  }

  scanner->advance(scanner);

  bool ok = parse_inner_list_element(scanner, 2, T_EXPLICIT_MARKUP_START);
  if (ok) {
    return true;
  }

  return false;
}

static bool parse_inner_list_element(RSTScanner* scanner, int consumed_chars, enum TokenType token_type)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!valid_symbols[token_type]) {
    return false;
  }

  if (is_space(scanner->lookahead)) {
    lexer->mark_end(lexer);
    lexer->result_symbol = token_type;

    // Set indent level to the first non-whitespace char
    int indent = scanner->back(scanner) + consumed_chars + get_indent_level(scanner);

    // If it's an empty line and T_EXPLICIT_MARKUP_START, then it's an empty comment.
    // Empty comments don't consume any following indented text.
    if (is_newline(scanner->lookahead) && token_type == T_EXPLICIT_MARKUP_START) {
      bool is_empty = true;
      scanner->advance(scanner);
      while (!is_newline(scanner->lookahead)) {
        if (!is_space(scanner->lookahead)) {
          is_empty = false;
          break;
        }
        scanner->advance(scanner);
      }
      if (is_empty && valid_symbols[T_EMPTY_COMMENT]) {
        lexer->result_symbol = T_EMPTY_COMMENT;
        return true;
      }
    } else if (token_type == T_EXPLICIT_MARKUP_START) {
      // Go to the next line.
      while (!is_newline(scanner->lookahead)) {
        scanner->advance(scanner);
      }
      scanner->advance(scanner);

      // The first non-empty line after the marker
      // determines the indentation of the body.
      while (true) {
        indent = get_indent_level(scanner);
        if (!is_newline(scanner->lookahead) || scanner->lookahead == CHAR_EOF) {
          break;
        }
        scanner->advance(scanner);
      }
      if (indent <= scanner->back(scanner)) {
        indent = scanner->back(scanner) + 1;
      }
    } else if (token_type == T_NUMERIC_BULLET) {
      // Check if the next line is an underline.
      // In that case, the list item is just a normal text from a title,
      // the underline must be at least as long as the title
      // https://github.com/stsewd/tree-sitter-rst/issues/30.

      // Go to the next line.
      consumed_chars = indent;
      while (!is_newline(scanner->lookahead)) {
        consumed_chars++;
        scanner->advance(scanner);
      }
      scanner->advance(scanner);

      // Check if that line is a valid underline.
      int32_t adornment = scanner->lookahead;
      int adornment_length = 0;
      if (is_adornment_char(adornment)) {
        while (true) {
          if (is_newline(scanner->lookahead)) {
            break;
          }
          if (scanner->lookahead != adornment) {
            adornment_length = -1;
            break;
          }
          adornment_length++;
          scanner->advance(scanner);
        }
      }
      if (adornment_length > 0 && adornment_length >= consumed_chars) {
        return parse_text(scanner, false);
      }
    }

    scanner->push(scanner, indent);
    return true;
  }

  return false;
}

static bool parse_field_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != ':' || !valid_symbols[T_FIELD_MARK]) {
    return false;
  }

  scanner->advance(scanner);
  lexer->mark_end(lexer);

  if (is_space(scanner->lookahead)) {
    return parse_text(scanner, true);
  }

  bool ok = parse_inner_field_mark(scanner);
  if (ok) {
    return true;
  }
  return parse_text(scanner, false);
}

static bool parse_inner_field_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!valid_symbols[T_FIELD_MARK]) {
    return false;
  }

  bool is_escaped = false;
  while (!is_newline(scanner->lookahead)) {
    if (scanner->lookahead == '/') {
      scanner->advance(scanner);
      is_escaped = true;
    } else {
      is_escaped = false;
    }

    if (scanner->lookahead == ':' && !is_space(scanner->previous) && !is_escaped) {
      scanner->advance(scanner);
      if (is_space(scanner->lookahead)) {
        break;
      }
    }

    scanner->advance(scanner);
  }

  if (scanner->previous == ':' && is_space(scanner->lookahead)) {
    lexer->result_symbol = T_FIELD_MARK;
    return true;
  }
  return false;
}

static bool parse_field_mark_end(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != ':' || !valid_symbols[T_FIELD_MARK_END]) {
    return false;
  }

  scanner->advance(scanner);
  lexer->mark_end(lexer);

  if (is_space(scanner->lookahead)) {
    // Consume all whitespaces.
    get_indent_level(scanner);
    // Go to the next line.
    while (!is_newline(scanner->lookahead)) {
      scanner->advance(scanner);
    }
    scanner->advance(scanner);

    // The first non-empty line after the field name marker
    // determines the indentation of the field body.
    int indent = 0;
    while (true) {
      indent = get_indent_level(scanner);
      if (!is_newline(scanner->lookahead) || scanner->lookahead == CHAR_EOF) {
        break;
      }
      scanner->advance(scanner);
    }
    if (indent > scanner->back(scanner)) {
      scanner->push(scanner, indent);
    } else {
      scanner->push(scanner, scanner->back(scanner) + 1);
    }

    lexer->result_symbol = T_FIELD_MARK_END;
    return true;
  }

  return parse_text(scanner, false);
}

static bool parse_label(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != '['
      || !(valid_symbols[T_FOOTNOTE_LABEL] || valid_symbols[T_CITATION_LABEL])) {
    return false;
  }
  scanner->advance(scanner);
  int type = parse_inner_label_name(scanner);
  if ((type == IM_CITATION_REFERENCE && valid_symbols[T_CITATION_LABEL])
      || (type == IM_FOOTNOTE_REFERENCE && valid_symbols[T_FOOTNOTE_LABEL])) {
    scanner->advance(scanner);
    if (is_space(scanner->lookahead)) {
      lexer->mark_end(lexer);
      if (type == IM_CITATION_REFERENCE) {
        lexer->result_symbol = T_CITATION_LABEL;
      } else if (type == IM_FOOTNOTE_REFERENCE) {
        lexer->result_symbol = T_FOOTNOTE_LABEL;
      }
      return true;
    }
  }

  return false;
}

static int parse_inner_label_name(RSTScanner* scanner)
{
  int type = -1;
  if (is_number(scanner->lookahead)) {
    while (is_number(scanner->lookahead)) {
      scanner->advance(scanner);
    }
    if (scanner->lookahead == ']') {
      type = IM_FOOTNOTE_REFERENCE;
    } else {
      if (parse_inner_alphanumeric_label(scanner)) {
        type = IM_CITATION_REFERENCE;
      }
    }
  } else if (scanner->lookahead == '*') {
    type = IM_FOOTNOTE_REFERENCE;
    scanner->advance(scanner);
  } else if (scanner->lookahead == '#') {
    scanner->advance(scanner);
    if (scanner->lookahead == ']') {
      type = IM_FOOTNOTE_REFERENCE;
    } else if (is_alphanumeric(scanner->lookahead)) {
      if (parse_inner_alphanumeric_label(scanner)) {
        type = IM_FOOTNOTE_REFERENCE;
      }
    }
  } else if (is_alphanumeric(scanner->lookahead)) {
    if (parse_inner_alphanumeric_label(scanner)) {
      type = IM_CITATION_REFERENCE;
    }
  } else {
    return -1;
  }

  if (scanner->lookahead == ']') {
    return type;
  }

  return -1;
}

static bool parse_inner_alphanumeric_label(RSTScanner* scanner)
{
  if (!(is_alphanumeric(scanner->lookahead)
          || is_internal_reference_char(scanner->lookahead))) {
    return false;
  }

  bool internal_symbol = false;
  while (is_alphanumeric(scanner->lookahead) || is_internal_reference_char(scanner->lookahead)) {
    if (is_internal_reference_char(scanner->lookahead)) {
      if (internal_symbol) {
        return false;
      }
      internal_symbol = true;
    } else {
      internal_symbol = false;
    }
    scanner->advance(scanner);
  }

  if (scanner->lookahead == ']') {
    return true;
  }

  return false;
}

static bool parse_target_name(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != '_' || !valid_symbols[T_TARGET_NAME]) {
    return false;
  }

  scanner->advance(scanner);

  if (scanner->lookahead == '_') {
    scanner->advance(scanner);
  } else if (scanner->lookahead == '`') {
    // Find ending "`:".
    while (true) {
      if (scanner->lookahead == '`') {
        scanner->advance(scanner);
        if (scanner->lookahead == ':') {
          break;
        }
      }

      if (is_newline(scanner->lookahead)) {
        break;
      }

      scanner->advance(scanner);
    }
  } else {
    bool is_escaped = false;
    while (true) {
      if (scanner->lookahead == '\\') {
        scanner->advance(scanner);
        is_escaped = true;
      } else {
        is_escaped = false;
      }

      if (is_newline(scanner->lookahead)) {
        break;
      }

      if (scanner->lookahead == ':' && !is_escaped) {
        break;
      }
      scanner->advance(scanner);
    }
  }

  // check for ': '
  if (scanner->lookahead != ':') {
    return false;
  }

  scanner->advance(scanner);

  if (is_space(scanner->lookahead)) {
    lexer->mark_end(lexer);
    lexer->result_symbol = T_TARGET_NAME;
    return true;
  }

  return false;
}

static bool parse_anonymous_target_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != '_' || !valid_symbols[T_ANONYMOUS_TARGET_MARK]) {
    return false;
  }

  scanner->advance(scanner);

  if (scanner->lookahead != '_') {
    return false;
  }

  scanner->advance(scanner);

  if (is_space(scanner->lookahead)) {
    lexer->mark_end(lexer);
    lexer->result_symbol = T_ANONYMOUS_TARGET_MARK;
    return true;
  }

  return false;
}

static bool parse_directive_name(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!is_alphanumeric(scanner->lookahead) || !valid_symbols[T_DIRECTIVE_NAME]) {
    return false;
  }

  scanner->advance(scanner);

  bool internal_symbol = false;
  bool keep_parsing = true;
  while (is_alphanumeric(scanner->lookahead)
      || is_internal_reference_char(scanner->lookahead)
      || (is_space(scanner->lookahead) && !is_newline(scanner->lookahead))) {
    // Directive names can have one (and only one) space before `::`.
    if (is_space(scanner->lookahead)) {
      lexer->mark_end(lexer);
      scanner->advance(scanner);
      scanner->advance(scanner);
      keep_parsing = false;
      break;
    }
    if (is_internal_reference_char(scanner->lookahead)) {
      if (internal_symbol) {
        break;
      }
      internal_symbol = true;
      lexer->mark_end(lexer);
    } else {
      internal_symbol = false;
    }
    scanner->advance(scanner);
  }

  if (scanner->lookahead != ':' || scanner->previous != ':') {
    return parse_text(scanner, keep_parsing);
  }
  scanner->advance(scanner);

  if (is_space(scanner->lookahead)) {
    lexer->result_symbol = T_DIRECTIVE_NAME;
    return true;
  }

  return false;
}

static bool parse_substitution_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != '|' || !valid_symbols[T_SUBSTITUTION_MARK]) {
    return false;
  }

  scanner->advance(scanner);

  if (!is_space(scanner->lookahead)) {
    bool ok = parse_inner_inline_markup(scanner, IM_SUBSTITUTION_REFERENCE);
    if (ok
        && lexer->result_symbol == T_SUBSTITUTION_REFERENCE
        && is_space(scanner->lookahead)
        && !is_newline(scanner->lookahead)) {
      lexer->result_symbol = T_SUBSTITUTION_MARK;
      return true;
    }
  }
  return false;
}

static bool parse_literal_block_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (scanner->lookahead != ':'
      || !(valid_symbols[T_LITERAL_INDENTED_BLOCK_MARK] || valid_symbols[T_LITERAL_QUOTED_BLOCK_MARK])) {
    return false;
  }

  scanner->advance(scanner);

  if (scanner->lookahead != ':') {
    if (valid_symbols[T_ROLE_NAME_PREFIX] || valid_symbols[T_ROLE_NAME_SUFFIX]) {
      return parse_inner_role(scanner);
    }
    return false;
  }

  scanner->advance(scanner);

  return parse_innner_literal_block_mark(scanner);
}

static bool parse_innner_literal_block_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!is_space(scanner->lookahead)
      || !(valid_symbols[T_LITERAL_INDENTED_BLOCK_MARK] || valid_symbols[T_LITERAL_QUOTED_BLOCK_MARK])) {
    return false;
  }

  lexer->mark_end(lexer);

  // Consume all whitespaces.
  while (is_space(scanner->lookahead) && !is_newline(scanner->lookahead)) {
    scanner->advance(scanner);
  }

  if (!is_newline(scanner->lookahead)) {
    return parse_text(scanner, false);
  }

  scanner->advance(scanner);

  // Make sure there is blank line
  while (!is_newline(scanner->lookahead)) {
    if (!is_space(scanner->lookahead)) {
      return false;
    }
    scanner->advance(scanner);
  }
  scanner->advance(scanner);

  // Skip all whitespaces and newlines and
  // get the indentation level of the first non-empty line.
  int indent = -1;
  while (scanner->lookahead != CHAR_EOF) {
    int local_indent = get_indent_level(scanner);
    if (!is_newline(scanner->lookahead)) {
      indent = local_indent;
      break;
    }
    scanner->advance(scanner);
  }

  // Literal blocks need to be indented, and quoted literal blocks
  // need to start with an adornment at the same indentation level.
  if (indent > scanner->back(scanner)) {
    scanner->push(scanner, scanner->back(scanner) + 1);
    lexer->result_symbol = T_LITERAL_INDENTED_BLOCK_MARK;
  } else if (indent == scanner->back(scanner) && is_adornment_char(scanner->lookahead)) {
    lexer->result_symbol = T_LITERAL_QUOTED_BLOCK_MARK;
  } else {
    return false;
  }
  if (!valid_symbols[lexer->result_symbol]) {
    return false;
  }
  return true;
}

static bool parse_quoted_literal_block(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!is_adornment_char(scanner->lookahead) || !valid_symbols[T_QUOTED_LITERAL_BLOCK]) {
    return false;
  }

  int32_t adornment = scanner->lookahead;
  int current_indent = scanner->back(scanner);

  while (true) {
    while (!is_newline(scanner->lookahead)) {
      scanner->advance(scanner);
    }
    lexer->mark_end(lexer);

    scanner->advance(scanner);

    // Check if it's an empty line or if the indentation was broken.
    int indent = get_indent_level(scanner);
    if (indent != current_indent || scanner->lookahead != adornment) {
      break;
    }

    if (scanner->lookahead != adornment) {
      return parse_text(scanner, false);
    }
  }
  lexer->result_symbol = T_QUOTED_LITERAL_BLOCK;
  return true;
}

static bool parse_line_block_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (scanner->lookahead != '|' || !valid_symbols[T_LINE_BLOCK_MARK]) {
    return false;
  }

  scanner->advance(scanner);

  if (is_space(scanner->lookahead)) {
    return parse_inner_list_element(scanner, 1, T_LINE_BLOCK_MARK);
  }

  return false;
}

static bool parse_attribution_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (!is_attribution_mark(scanner->lookahead) || !valid_symbols[T_ATTRIBUTION_MARK]) {
    return false;
  }

  int consumed_chars = 0;
  if (scanner->lookahead == '-') {
    while (scanner->lookahead == '-') {
      consumed_chars++;
      scanner->advance(scanner);
    }

    if (consumed_chars < 2 || consumed_chars > 3) {
      return false;
    }
  } else {
    scanner->advance(scanner);
    consumed_chars++;
  }

  return parse_inner_list_element(scanner, consumed_chars, T_ATTRIBUTION_MARK);
}

static bool parse_doctest_block_mark(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != '>' || !valid_symbols[T_DOCTEST_BLOCK_MARK]) {
    return false;
  }

  int consumed_chars = 0;
  while (scanner->lookahead == '>') {
    consumed_chars++;
    scanner->advance(scanner);
  }

  if (consumed_chars == 3 && is_space(scanner->lookahead)) {
    lexer->mark_end(lexer);
    lexer->result_symbol = T_DOCTEST_BLOCK_MARK;
    return true;
  }
  return false;
}

static bool parse_inline_markup(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;
  scanner->advance(scanner);
  lexer->mark_end(lexer);

  unsigned type = 0;

  if (scanner->previous == '*' && scanner->lookahead == '*' && valid_symbols[T_STRONG]) {
    type = IM_STRONG;
  } else if (scanner->previous == '*' && valid_symbols[T_EMPHASIS]) {
    type = IM_EMPHASIS;
  } else if (scanner->previous == '`' && scanner->lookahead == '`' && valid_symbols[T_LITERAL]) {
    type = IM_LITERAL;
  } else if (scanner->previous == '`' && (valid_symbols[T_INTERPRETED_TEXT] || valid_symbols[T_INTERPRETED_TEXT_PREFIX] || valid_symbols[T_REFERENCE])) {
    type = IM_INTERPRETED_TEXT | IM_INTERPRETED_TEXT_PREFIX | IM_REFERENCE;
  } else if (scanner->previous == '|' && valid_symbols[T_SUBSTITUTION_REFERENCE]) {
    type = IM_SUBSTITUTION_REFERENCE;
  } else if (scanner->previous == '_' && scanner->lookahead == '`' && valid_symbols[T_INLINE_TARGET]) {
    type = IM_INLINE_TARGET;
  } else if (scanner->previous == '[' && (valid_symbols[T_FOOTNOTE_REFERENCE] || valid_symbols[T_CITATION_REFERENCE])) {
    type = IM_FOOTNOTE_REFERENCE | IM_CITATION_REFERENCE;
  }

  // Skip one char for tokens that start with a double char
  if (type & (IM_STRONG | IM_LITERAL | IM_INLINE_TARGET)) {
    scanner->advance(scanner);
  }

  // The next character can't be a white space
  if (is_space(scanner->lookahead)) {
    if (type & IM_EMPHASIS) {
      bool ok = parse_inner_list_element(scanner, 1, T_CHAR_BULLET);
      if (ok) {
        return true;
      }
    }
    if (valid_symbols[T_TEXT]) {
      lexer->mark_end(lexer);
      lexer->result_symbol = T_TEXT;
      return true;
    }
    return false;
  }

  return parse_inner_inline_markup(scanner, type);
}

static bool parse_inner_inline_markup(RSTScanner* scanner, unsigned type)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  int consumed_chars = 0;
  bool word_found = false;
  bool is_escaped = false;

  if (type & IM_FOOTNOTE_REFERENCE || type & IM_CITATION_REFERENCE) {
    int final_type = parse_inner_label_name(scanner);
    if ((final_type == IM_FOOTNOTE_REFERENCE && type & IM_FOOTNOTE_REFERENCE)
        || (final_type == IM_CITATION_REFERENCE && type & IM_CITATION_REFERENCE)) {
      scanner->advance(scanner);
      if (scanner->lookahead == '_') {
        scanner->advance(scanner);
        if (is_space(scanner->lookahead) || is_end_char(scanner->lookahead)) {
          lexer->mark_end(lexer);
          if (final_type == IM_CITATION_REFERENCE) {
            lexer->result_symbol = T_CITATION_REFERENCE;
          } else if (final_type == IM_FOOTNOTE_REFERENCE) {
            lexer->result_symbol = T_FOOTNOTE_REFERENCE;
          }
          return true;
        }
      }
    }
    return parse_text(scanner, false);
  }

  while (scanner->lookahead != CHAR_EOF) {
    // Skip indentation
    if (is_newline(scanner->lookahead)) {
      if (!word_found) {
        word_found = true;
        lexer->mark_end(lexer);
      }
      scanner->advance(scanner);
      int indent = get_indent_level(scanner);
      if (indent != scanner->back(scanner) || is_newline(scanner->lookahead)) {
        break;
      }
    }

    // Skip escaped chars
    if (scanner->lookahead == '\\') {
      is_escaped = true;
      scanner->advance(scanner);
      if (is_newline(scanner->lookahead)) {
        break;
      }
    } else {
      is_escaped = false;
    }

    // Mark the end of the word if a space was found
    if (!word_found && is_space(scanner->lookahead)) {
      word_found = true;
      lexer->mark_end(lexer);
    }

    // Mark the end of the word if a start char was found
    if (!word_found && is_start_char(scanner->lookahead)) {
      word_found = true;
      lexer->mark_end(lexer);
    }

    // Check if it's a terminal character
    if (consumed_chars > 0 && !is_space(scanner->previous) && is_inline_markup_end_char(scanner->lookahead)
        // Literal is the only inline markup that doesn't care if the previous
        // char is '\'
        && (!is_escaped || (type & IM_LITERAL))) {
      scanner->advance(scanner);

      bool is_valid = true;
      bool advance = false;

      if ((type & IM_STRONG) && scanner->previous == '*' && scanner->lookahead == '*') {
        lexer->result_symbol = T_STRONG;
        while (scanner->lookahead == '*') {
          scanner->advance(scanner);
          consumed_chars++;
        }
      } else if ((type & IM_EMPHASIS) && scanner->previous == '*') {
        lexer->result_symbol = T_EMPHASIS;
      } else if ((type & IM_LITERAL) && scanner->previous == '`' && scanner->lookahead == '`') {
        lexer->result_symbol = T_LITERAL;
        while (scanner->lookahead == '`') {
          scanner->advance(scanner);
          consumed_chars++;
        }
      } else if ((type & IM_INLINE_TARGET) && scanner->previous == '`') {
        lexer->result_symbol = T_INLINE_TARGET;
      } else if ((type & IM_REFERENCE) && scanner->previous == '`' && scanner->lookahead == '_') {
        lexer->result_symbol = T_REFERENCE;

        // Check for annonymous references
        scanner->advance(scanner);
        consumed_chars++;
        if (scanner->lookahead == '_') {
          advance = true;
        }
      } else if ((type & IM_INTERPRETED_TEXT || type & IM_INTERPRETED_TEXT_PREFIX) && scanner->previous == '`') {
        if (scanner->lookahead == ':' && type & IM_INTERPRETED_TEXT_PREFIX && valid_symbols[T_INTERPRETED_TEXT_PREFIX]) {
          lexer->mark_end(lexer);
          scanner->advance(scanner);
          bool ok = parse_role_name(scanner);
          if (ok) {
            lexer->result_symbol = T_INTERPRETED_TEXT_PREFIX;
            return true;
          }
          if (valid_symbols[T_INTERPRETED_TEXT]) {
            lexer->result_symbol = T_INTERPRETED_TEXT;
            return true;
          }
          is_valid = false;
        } else {
          lexer->result_symbol = T_INTERPRETED_TEXT;
        }
      } else if ((type & IM_SUBSTITUTION_REFERENCE) && scanner->previous == '|') {
        lexer->result_symbol = T_SUBSTITUTION_REFERENCE;
        // Substitution references can end with '__'.
        if (scanner->lookahead == '_') {
          scanner->advance(scanner);
          if (scanner->lookahead == '_') {
            advance = true;
          }
        }
      } else {
        is_valid = false;
      }

      if (advance) {
        scanner->advance(scanner);
        consumed_chars++;
      }

      // The next char should be a whitespace or an end char
      if (is_valid && (is_space(scanner->lookahead) || is_end_char(scanner->lookahead))) {
        lexer->mark_end(lexer);
        return true;
      }
    } else {
      scanner->advance(scanner);
    }

    consumed_chars++;
  }

  if (!word_found && is_newline(scanner->lookahead)) {
    return parse_text(scanner, true);
  }
  return parse_text(scanner, false);
}

static bool parse_reference(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (is_space(scanner->lookahead) || is_internal_reference_char(scanner->lookahead) || !valid_symbols[T_REFERENCE]) {
    return false;
  }
  scanner->advance(scanner);
  return parse_inner_reference(scanner);
}

static bool parse_inner_reference(RSTScanner* scanner)
{
  TSLexer* lexer = scanner->lexer;

  bool internal_symbol = is_internal_reference_char(scanner->previous);
  bool is_word = false;
  while ((!is_space(scanner->lookahead) && !is_end_char(scanner->lookahead)) || is_internal_reference_char(scanner->lookahead)) {
    // Mark the end of the worl?d.
    if (is_start_char(scanner->lookahead) && !is_word) {
      is_word = true;
      lexer->mark_end(lexer);
    }
    if (is_internal_reference_char(scanner->lookahead)) {
      if (internal_symbol) {
        break;
      }
      internal_symbol = true;
    } else {
      internal_symbol = false;
    }
    scanner->advance(scanner);
  }

  // Only an anonymous reference can contain
  // and end with two consecutive '_'.
  if (scanner->lookahead == '_' && scanner->previous == '_') {
    scanner->advance(scanner);
  }

  if (scanner->previous == '_' && (is_space(scanner->lookahead) || is_end_char(scanner->lookahead))) {
    lexer->mark_end(lexer);
    lexer->result_symbol = T_REFERENCE;
    return true;
  }

  return parse_text(scanner, !is_word);
}

static bool parse_standalone_hyperlink(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;

  if (!is_abc(scanner->lookahead) || !valid_symbols[T_STANDALONE_HYPERLINK]) {
    return false;
  }
  scanner->advance(scanner);
  return parse_inner_standalone_hyperlink(scanner);
}

static bool parse_inner_standalone_hyperlink(RSTScanner* scanner)
{
  TSLexer* lexer = scanner->lexer;

  const unsigned MAX_SCHEMA_LEN = 20;
  char* schema = malloc(sizeof(char) * MAX_SCHEMA_LEN);
  unsigned consumed_chars = 0;

  // TODO: cast this more safely
  schema[consumed_chars++] = (char)scanner->previous;
  while (consumed_chars < MAX_SCHEMA_LEN) {
    if (!is_alphanumeric(scanner->lookahead)) {
      break;
    }
    // TODO: cast this more safely
    schema[consumed_chars++] = (char)scanner->lookahead;
    scanner->advance(scanner);
  }

  bool is_word = false;
  if (is_start_char(scanner->lookahead)) {
    lexer->mark_end(lexer);
  }

  bool is_valid = false;
  if (scanner->lookahead == ':') {
    is_valid = is_known_schema(schema, consumed_chars);
  } else if (scanner->lookahead == '@') {
    is_valid = true;
  }

  // Clean up
  free(schema);
  schema = NULL;

  if (!is_valid) {
    if ((!is_space(scanner->lookahead) && !is_end_char(scanner->lookahead)) || is_internal_reference_char(scanner->lookahead)) {
      return parse_inner_reference(scanner);
    }

    return parse_text(scanner, !is_word);
  }

  scanner->advance(scanner);

  if (scanner->lookahead == '/') {
    scanner->advance(scanner);
  } else if (!is_alphanumeric(scanner->lookahead)) {
    return parse_text(scanner, !is_word);
  }

  consumed_chars = 0;
  bool is_escaped = false;
  while (true) {
    lexer->mark_end(lexer);
    if (scanner->lookahead == '\\') {
      scanner->advance(scanner);
      is_escaped = true;
    } else {
      is_escaped = false;
    }
    if (is_invalid_uri_char(scanner->lookahead)) {
      break;
    }
    if (is_space(scanner->lookahead)
        || (is_end_char(scanner->lookahead) && !is_escaped && scanner->lookahead != '/')) {
      if (is_end_char(scanner->lookahead)) {
        lexer->mark_end(lexer);
        scanner->advance(scanner);
        if (!is_alphanumeric(scanner->lookahead)) {
          lexer->result_symbol = T_STANDALONE_HYPERLINK;
          return true;
        }
      } else {
        break;
      }
    }
    scanner->advance(scanner);
    consumed_chars++;
  }

  if (consumed_chars > 0) {
    lexer->result_symbol = T_STANDALONE_HYPERLINK;
    return true;
  }

  return parse_text(scanner, !is_word);
}

static bool parse_role(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (scanner->lookahead != ':'
      || (!valid_symbols[T_ROLE_NAME_SUFFIX] && !valid_symbols[T_ROLE_NAME_PREFIX])) {
    return false;
  }

  scanner->advance(scanner);

  // Mark the end of the word before going deeper.
  lexer->mark_end(lexer);

  if (is_space(scanner->lookahead) && valid_symbols[T_FIELD_MARK_END]) {
    // Consume all whitespaces.
    get_indent_level(scanner);
    lexer->mark_end(lexer);
    // Go to the next line
    while (!is_newline(scanner->lookahead)) {
      scanner->advance(scanner);
    }
    scanner->advance(scanner);

    // The first non-empty line after the field name marker
    // determines the indentation of the field body.
    int indent = 0;
    while (true) {
      indent = get_indent_level(scanner);
      if (!is_newline(scanner->lookahead) || scanner->lookahead == CHAR_EOF) {
        break;
      }
      scanner->advance(scanner);
    }
    if (indent > scanner->back(scanner)) {
      scanner->push(scanner, indent);
    } else {
      scanner->push(scanner, scanner->back(scanner) + 1);
    }

    lexer->result_symbol = T_FIELD_MARK_END;
    return true;
  }

  if (is_alphanumeric(scanner->lookahead)) {
    bool ok = parse_inner_role(scanner);
    if (ok) {
      return true;
    }
  }

  return parse_text(scanner, false);
}

static bool parse_inner_role(RSTScanner* scanner)
{
  const bool* valid_symbols = scanner->valid_symbols;
  TSLexer* lexer = scanner->lexer;

  if (!is_alphanumeric(scanner->lookahead)
      || (!valid_symbols[T_ROLE_NAME_SUFFIX] && !valid_symbols[T_ROLE_NAME_PREFIX])) {
    return false;
  }
  // Mark the end at the previous character (`:`),
  // so that the role name is not included in the T_FIELD_MARK token.
  lexer->mark_end(lexer);
  bool ok = parse_role_name(scanner);
  if (ok) {
    if (scanner->lookahead == '`' && valid_symbols[T_ROLE_NAME_PREFIX]) {
      lexer->mark_end(lexer);
      lexer->result_symbol = T_ROLE_NAME_PREFIX;
      return true;
    }

    if (is_space(scanner->lookahead) && valid_symbols[T_FIELD_MARK]) {
      lexer->result_symbol = T_FIELD_MARK;
      return true;
    }

    if ((is_space(scanner->lookahead) || is_end_char(scanner->lookahead))
        && valid_symbols[T_ROLE_NAME_SUFFIX]) {
      lexer->mark_end(lexer);
      lexer->result_symbol = T_ROLE_NAME_SUFFIX;
      return true;
    }
  }
  if (valid_symbols[T_FIELD_MARK]) {
    ok = parse_inner_field_mark(scanner);
    if (ok) {
      return true;
    }
  }
  return false;
}

/// This function assumes the previous token was `:` and it's already consumed.
static bool parse_role_name(RSTScanner* scanner)
{
  if (!is_alphanumeric(scanner->lookahead)) {
    return false;
  }

  bool internal_symbol = true;
  while (is_alphanumeric(scanner->lookahead) || is_internal_reference_char(scanner->lookahead)) {
    if (is_internal_reference_char(scanner->lookahead)) {
      if (internal_symbol) {
        return false;
      }
      internal_symbol = true;
    } else {
      internal_symbol = false;
    }
    scanner->advance(scanner);
  }

  if (scanner->previous == ':') {
    return true;
  }

  return false;
}

/// Parse normal text.
///
/// Text nodes are separated by white spaces or a start char like `(`
static bool parse_text(RSTScanner* scanner, bool mark_end)
{
  TSLexer* lexer = scanner->lexer;
  const bool* valid_symbols = scanner->valid_symbols;
  if (!valid_symbols[T_TEXT]) {
    return false;
  }

  if (is_start_char(scanner->lookahead)) {
    scanner->advance(scanner);
  } else {
    while (!is_space(scanner->lookahead)) {
      if (is_start_char(scanner->lookahead)) {
        break;
      }
      scanner->advance(scanner);
    }
  }

  if (mark_end) {
    lexer->mark_end(lexer);
  }
  lexer->result_symbol = T_TEXT;
  return true;
}
