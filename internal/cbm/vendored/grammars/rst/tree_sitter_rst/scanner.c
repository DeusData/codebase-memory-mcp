#include "scanner.h"

#include <stdio.h>
#include <string.h>

#include "chars.c"
#include "parser.c"
#include "tokens.h"

/// Build a new `RSTScanner` object.
///
/// `destroy_rst_scanner` should be called to safely destroy this object.
static RSTScanner* new_rst_scanner()
{
  RSTScanner* scanner = malloc(sizeof(RSTScanner));

  scanner->advance = rst_scanner_advance;
  scanner->skip = rst_scanner_skip;

  scanner->indent_stack = malloc(sizeof(int) * RST_SCANNER_STACK_MAX_CAPACITY);
  scanner->length = 0;

  scanner->push = rst_scanner_push;
  scanner->pop = rst_scanner_pop;
  scanner->back = rst_scanner_back;

  scanner->serialize = rst_scanner_serialize;
  scanner->deserialize = rst_scanner_deserialize;
  scanner->scan = rst_scanner_scan;

  return scanner;
}

static void destroy_rst_scanner(RSTScanner* scanner)
{
  free(scanner->indent_stack);
  free(scanner);
}

static void rst_scanner_advance(RSTScanner* scanner)
{
  TSLexer* lexer = scanner->lexer;
  scanner->previous = scanner->lookahead;
  lexer->advance(lexer, false);
  // Skip over the \r char in \r\n.
  if (lexer->lookahead == CHAR_CARRIAGE_RETURN) {
    lexer->advance(lexer, false);
  }
  scanner->lookahead = lexer->lookahead;
}

static void rst_scanner_skip(RSTScanner* scanner)
{
  TSLexer* lexer = scanner->lexer;
  scanner->previous = scanner->lookahead;
  lexer->advance(lexer, true);
  scanner->lookahead = lexer->lookahead;
}

static void rst_scanner_push(RSTScanner* scanner, int value)
{
  if (scanner->length >= RST_SCANNER_STACK_MAX_CAPACITY) {
    return;
  }
  scanner->indent_stack[scanner->length++] = value;
}

static int rst_scanner_pop(RSTScanner* scanner)
{
  if (scanner->length <= 0) {
    return 0;
  }
  return scanner->indent_stack[--scanner->length];
}

static int rst_scanner_back(const RSTScanner* scanner)
{
  if (scanner->length <= 0) {
    return 0;
  }
  return scanner->indent_stack[scanner->length - 1];
}

static unsigned rst_scanner_serialize(RSTScanner* scanner, char* buffer)
{
  unsigned i = scanner->length;
  if (i > TREE_SITTER_SERIALIZATION_BUFFER_SIZE) {
    i = TREE_SITTER_SERIALIZATION_BUFFER_SIZE;
  }
  memcpy(buffer, scanner->indent_stack, i);
  return i;
}

static void rst_scanner_deserialize(RSTScanner* scanner, const char* buffer, unsigned length)
{
  if (buffer != NULL && length > 0) {
    memcpy((void*)buffer, scanner->indent_stack, length);
    scanner->length = length;
  } else {
    scanner->length = 0;
  }
}

static bool rst_scanner_scan(RSTScanner* scanner)
{
  TSLexer* lexer = scanner->lexer;
  const bool* valid_symbols = scanner->valid_symbols;
  int32_t current = lexer->lookahead;

  // If all valid symbols are true, tree-sitter is in correction mode,
  // we fallback to parse the content as a text node.
  // TODO: We don't want to parse anything in this case,
  // we should move the text node parsing to JS.
  if (valid_symbols[T_INVALID_TOKEN]) {
    if (!is_space(current) && valid_symbols[T_TEXT]) {
      return parse_text(scanner, true);
    }
    return false;
  }

  if (is_adornment_char(current)
      && (valid_symbols[T_OVERLINE] || valid_symbols[T_TRANSITION])) {
    return parse_overline(scanner);
  }

  if (is_adornment_char(current)
      && (valid_symbols[T_UNDERLINE] || valid_symbols[T_TRANSITION])) {
    return parse_underline(scanner);
  }

  if (is_adornment_char(current) && valid_symbols[T_QUOTED_LITERAL_BLOCK]) {
    return parse_quoted_literal_block(scanner);
  }

  if (current == '.' && valid_symbols[T_EXPLICIT_MARKUP_START]) {
    return parse_explict_markup_start(scanner);
  }

  if (is_attribution_mark(current) && valid_symbols[T_ATTRIBUTION_MARK]) {
    return parse_attribution_mark(scanner);
  }

  if (current == '['
      && (valid_symbols[T_FOOTNOTE_LABEL] || valid_symbols[T_CITATION_LABEL])) {
    return parse_label(scanner);
  }

  if (current == '_' && valid_symbols[T_TARGET_NAME]) {
    return parse_target_name(scanner);
  }

  if (current == '_' && valid_symbols[T_ANONYMOUS_TARGET_MARK]) {
    return parse_anonymous_target_mark(scanner);
  }

  if (current == '|' && valid_symbols[T_SUBSTITUTION_MARK]) {
    return parse_substitution_mark(scanner);
  }

  if (current == '|' && valid_symbols[T_LINE_BLOCK_MARK]) {
    return parse_line_block_mark(scanner);
  }

  if (current == '>' && valid_symbols[T_DOCTEST_BLOCK_MARK]) {
    return parse_doctest_block_mark(scanner);
  }

  if (is_alphanumeric(current) && valid_symbols[T_DIRECTIVE_NAME]) {
    return parse_directive_name(scanner);
  }

  if (is_inline_markup_start_char(current)
      && (valid_symbols[T_EMPHASIS]
          || valid_symbols[T_STRONG]
          || valid_symbols[T_INTERPRETED_TEXT]
          || valid_symbols[T_INTERPRETED_TEXT_PREFIX]
          || valid_symbols[T_LITERAL]
          || valid_symbols[T_SUBSTITUTION_REFERENCE]
          || valid_symbols[T_INLINE_TARGET]
          || valid_symbols[T_FOOTNOTE_REFERENCE]
          || valid_symbols[T_CITATION_REFERENCE]
          || valid_symbols[T_REFERENCE])) {
    return parse_inline_markup(scanner);
  }

  if ((is_numeric_bullet(current) || current == '(')
      && valid_symbols[T_NUMERIC_BULLET]) {
    return parse_numeric_bullet(scanner);
  }

  if (is_char_bullet(current) && valid_symbols[T_CHAR_BULLET]) {
    return parse_char_bullet(scanner);
  }

  if (current == ':' && (valid_symbols[T_LITERAL_INDENTED_BLOCK_MARK] || valid_symbols[T_LITERAL_QUOTED_BLOCK_MARK])) {
    return parse_literal_block_mark(scanner);
  }

  if (current == ':' && (valid_symbols[T_ROLE_NAME_PREFIX] || valid_symbols[T_ROLE_NAME_SUFFIX])) {
    return parse_role(scanner);
  }

  if (current == ':' && valid_symbols[T_FIELD_MARK]) {
    return parse_field_mark(scanner);
  }

  if (current == ':' && valid_symbols[T_FIELD_MARK_END]) {
    return parse_field_mark_end(scanner);
  }

  if (is_abc(current) && valid_symbols[T_STANDALONE_HYPERLINK]) {
    return parse_standalone_hyperlink(scanner);
  }

  if (!is_space(current)
      && !is_internal_reference_char(current)
      && !is_start_char(current)
      && !is_end_char(current)
      && valid_symbols[T_REFERENCE]) {
    return parse_reference(scanner);
  }

  if (!is_space(current) && valid_symbols[T_TEXT]) {
    return parse_text(scanner, true);
  }

  if (is_space(current)) {
    return parse_indent(scanner);
  }

  return false;
}
