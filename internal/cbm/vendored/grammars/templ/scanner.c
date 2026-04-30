// Based on
// https://github.com/tree-sitter/tree-sitter-html/blob/master/src/scanner.c

#include <string.h>
#include <wctype.h>

#include "tree_sitter/parser.h"

// Helper struct to have multicharacter lookahead

#define LOOKAHEAD_BUFFER_SIZE 16

typedef struct {
  int buf[LOOKAHEAD_BUFFER_SIZE];
  size_t write_pos;
} LookaheadBuffer;

static void lookahead_buffer_init(LookaheadBuffer *buffer) {
  memset(&buffer->buf[0], 0, LOOKAHEAD_BUFFER_SIZE);
  buffer->write_pos = 0;
}

// static void lookahead_buffer_dump(LookaheadBuffer *buffer) {
//   printf("\"");
//   for (size_t i = 0; i < buffer->write_pos; i++) {
//     printf("%c", buffer->buf[i]);
//   }
//   printf("\"\n");
// }

static bool lookahead_buffer_find_char(LookaheadBuffer *buffer,
                                       bool (*callback)(int ch)) {
  for (size_t i = 0; i < buffer->write_pos; i++) {
    if (callback(buffer->buf[i])) {
      return true;
    }
  }

  return false;
}

// Tries to find the keyword `str` in the character stream of `lexer`.
//
// Since TSLexer doesn't allow backtracking and we need it to lookup
// different keywords, we have to implement backtracking ourselves.
//
// It's relatively simple:
// * if we have any buffered data, try it first
// * otherwise pull from the stream while simultaneously adding to the
// buffer
//
// The next call will have the buffer populated.
static bool lookahead_buffer_find_keyword(LookaheadBuffer *buffer,
                                          TSLexer *lexer, const char *str) {
  size_t length = strlen(str);

  // First look in the buffer
  for (size_t i = 0; i < buffer->write_pos && i < length; i++) {
    if (buffer->buf[i] != str[i]) {
      return false;
    }

    length--;
  }

  const char *str_remaining = &str[buffer->write_pos];

  // Otherwise fetch data from the lexer
  for (size_t i = 0; i < length; i++) {
    if (lexer->eof(lexer) || lexer->lookahead != str_remaining[i]) {
      return false;
    }

    buffer->buf[buffer->write_pos] = lexer->lookahead;
    buffer->write_pos++;

    lexer->advance(lexer, false);
  }

  return true;
}

//

enum TokenType {
  CSS_PROPERTY_VALUE,
  SCRIPT_BLOCK_TEXT,
  SWITCH_ELEMENT_TEXT,
  ELEMENT_TEXT,
};

typedef struct {
  bool saw_at_symbol;
} Scanner;

static unsigned serialize(Scanner *scanner, char *buffer) {
  buffer[0] = scanner->saw_at_symbol ? 1 : 0;

  return 0;
}

static void deserialize(Scanner *scanner, const char *buffer, unsigned length) {
  if (length <= 0) {
    return;
  }

  if (buffer[0] == 1) {
    scanner->saw_at_symbol = true;
  } else {
    scanner->saw_at_symbol = false;
  }
}

static bool scan_css_property_value(Scanner *scanner, TSLexer *lexer) {
  (void)scanner;

  // If we encounter the start of a templ expression, bail
  if (lexer->lookahead == '{') {
    return false;
  }

  lexer->result_symbol = CSS_PROPERTY_VALUE;

  // Consume everything until we find the end of the value;
  while (!lexer->eof(lexer)) {
    if (lexer->lookahead == ';') {
      return true;
    }
    lexer->advance(lexer, false);
  }

  return false;
}

static bool is_element_text_terminator(int ch) {
  switch (ch) {
  case '<':
  case '{':
  case '}':
  case '\n':
    return true;
  }

  return false;
}

static bool is_element_text_terminator_for_import_expression(int ch) {
  switch (ch) {
  case '.':
  case '(':
  case ')':
  case '[':
    return true;
  }

  return false;
}

const char *statement_keywords[] = {
    // Comments
    "//",
    "/*",
    // Other statements
    "if ",
    "else ",
    "for ",
    "switch ",
    // Switch keywords
    "case ",
    "default:",
};
const size_t statement_keywords_count =
    sizeof(statement_keywords) / sizeof(const char *) - 2;
const size_t switch_statement_keywords_count =
    sizeof(statement_keywords) / sizeof(const char *);

static bool scan_element_text(Scanner *scanner, TSLexer *lexer,
                              bool in_switch) {
  int symbol = in_switch ? SWITCH_ELEMENT_TEXT : ELEMENT_TEXT;
  lexer->result_symbol = symbol;

  size_t keywords_count =
      (in_switch) ? switch_statement_keywords_count : statement_keywords_count;

  // Start by marking the end so the following calls to advance don't
  // increase the token size
  lexer->mark_end(lexer);

  LookaheadBuffer buffer;
  lookahead_buffer_init(&buffer);

  bool has_marked = false;
  size_t count = 0;

  if (lexer->eof(lexer)) {
    return false;
  }

  // Detect if the node starts with a keyword that makes it a statement instead.
  for (size_t i = 0; i < keywords_count; i++) {
    const char *keyword = statement_keywords[i];

    // Since we're looking for a multicharacter token we need backtracking but
    // TSLexer doesn't provide it so we have to do it ourselves.
    if (lookahead_buffer_find_keyword(&buffer, lexer, keyword)) {
      goto done;
    }
  }

  // Try for a "@" which signals a component import expression
  if (lookahead_buffer_find_keyword(&buffer, lexer, "@")) {
    scanner->saw_at_symbol = true;
    goto done;
  }

  // 2. We looked for a statement keyword but found none.

  // Process the remaining data in the buffer to look for terminator characters.
  if (lookahead_buffer_find_char(&buffer, is_element_text_terminator)) {
    goto done;
  }

  // If we saw a @ symbol, we could be in an import expression and the
  // terminator characters differ.
  if (scanner->saw_at_symbol) {
    if (lookahead_buffer_find_char(
            &buffer, is_element_text_terminator_for_import_expression)) {
      goto done;
    }
  }

  // Everything up to this
  count += buffer.write_pos;

  // 3. We looked for a terminator in the buffer but found none. Now we can
  // start processing the lexer stream itself.
  //
  // There's no need for backtracking here since we only need a single character
  // lookahead.

  while (!lexer->eof(lexer)) {
    if (is_element_text_terminator(lexer->lookahead)) {
      goto done;
    }
    if (scanner->saw_at_symbol &&
        is_element_text_terminator_for_import_expression(lexer->lookahead)) {
      goto done;
    }

    lexer->advance(lexer, false);
    lexer->mark_end(lexer);
    count++;
  }

done:

  if (count > 0) {
    lexer->mark_end(lexer);
    has_marked = true;
  }

  /* printf("done: %b, chars: %zu\n", has_marked, count); */

  if (has_marked) {
    scanner->saw_at_symbol = false;
  }

  return has_marked;
}

static bool scan_script_block_text(Scanner *scanner, TSLexer *lexer) {
  (void)scanner;

  lexer->result_symbol = SCRIPT_BLOCK_TEXT;

  // Start by marking the end so the following calls to advance don't
  // increase the token size
  lexer->mark_end(lexer);

  if (lexer->eof(lexer)) {
    return false;
  }

  bool has_marked = false;

  int brace_count = 1;
  int count = 0;

  while (!lexer->eof(lexer)) {
    switch (lexer->lookahead) {
    case '{':
      brace_count++;
      break;
    case '}':
      brace_count--;
      if (brace_count == 0) {
        goto done;
      }
      break;
    }

    lexer->advance(lexer, false);
    lexer->mark_end(lexer);

    has_marked = true;
    count++;
  }

done:

  (void)count;
  /* printf("done: %d, count: %d\n", has_marked, count); */

  return has_marked;
}

static bool scan(Scanner *scanner, TSLexer *lexer, const bool *valid_symbols) {
  while (!lexer->eof(lexer) && iswspace(lexer->lookahead)) {
    lexer->advance(lexer, true);
  }

  if (valid_symbols[CSS_PROPERTY_VALUE] &&
      scan_css_property_value(scanner, lexer)) {
    return true;
  }

  if (valid_symbols[SCRIPT_BLOCK_TEXT] &&
      scan_script_block_text(scanner, lexer)) {
    return true;
  }

  if (valid_symbols[SWITCH_ELEMENT_TEXT] &&
      scan_element_text(scanner, lexer, true)) {
    return true;
  }

  if (valid_symbols[ELEMENT_TEXT] && scan_element_text(scanner, lexer, false)) {
    return true;
  }

  return false;
}

void *tree_sitter_templ_external_scanner_create() {
  Scanner *scanner = (Scanner *)calloc(1, sizeof(Scanner));

  scanner->saw_at_symbol = false;

  return scanner;
}

bool tree_sitter_templ_external_scanner_scan(void *payload, TSLexer *lexer,
                                             const bool *valid_symbols) {
  Scanner *scanner = (Scanner *)payload;
  return scan(scanner, lexer, valid_symbols);
}

unsigned tree_sitter_templ_external_scanner_serialize(void *payload,
                                                      char *buffer) {
  Scanner *scanner = (Scanner *)payload;
  return serialize(scanner, buffer);
}

void tree_sitter_templ_external_scanner_deserialize(void *payload,
                                                    const char *buffer,
                                                    unsigned length) {
  Scanner *scanner = (Scanner *)payload;
  deserialize(scanner, buffer, length);
}

void tree_sitter_templ_external_scanner_destroy(void *payload) {
  Scanner *scanner = (Scanner *)payload;
  free(scanner);
}
