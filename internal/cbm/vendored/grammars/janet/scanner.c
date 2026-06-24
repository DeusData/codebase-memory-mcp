#include "tree_sitter/parser.h"
#include <wctype.h>

enum TokenType {
  LONG_BUF_LIT,
  LONG_STR_LIT
};

void* tree_sitter_janet_simple_external_scanner_create(
    void
)
{
  return NULL;
}

void tree_sitter_janet_simple_external_scanner_destroy(
  void* payload
)
{
}

void tree_sitter_janet_simple_external_scanner_reset(
  void* payload
)
{
}

unsigned tree_sitter_janet_simple_external_scanner_serialize(
  void* payload,
  char* buffer
)
{
  return 0;
}

void tree_sitter_janet_simple_external_scanner_deserialize(
  void *payload,
  const char *buffer,
  unsigned length
)
{
}

bool tree_sitter_janet_simple_external_scanner_scan(
  void *payload,
  TSLexer *lexer,
  const bool *valid_symbols
)
{
  // skip a bit brother
  while (iswspace(lexer->lookahead)) {
    lexer->advance(lexer, true);
  }
  // there can be only...two?
  if (valid_symbols[LONG_BUF_LIT] || valid_symbols[LONG_STR_LIT]) {
    // so which one was it?
    if (lexer->lookahead == '@') {
      lexer->result_symbol = LONG_BUF_LIT;
      lexer->advance(lexer, false);
    } else {
      lexer->result_symbol = LONG_STR_LIT;
    }
    // * long strings start with one or more backticks
    // * for a long buffer, the leading @ has been skipped (see above)
    //   to arrive at the first backtick
    // consume the first backtick
    if (lexer->lookahead != '`') {
      return false;
    }
    // getting here means a backtick was encountered
    lexer->advance(lexer, false);
    uint32_t n_backticks = 1;
    // arrive at a total number of backticks
    for (;;) {
      if (lexer->eof(lexer)) {
        return false;
      }
      // found one!
      if (lexer->lookahead == '`') {
        n_backticks++;
        lexer->advance(lexer, false);
        continue;
      } else { // nope, time to bail
        break;
      }
    }
    // getting here means the last character examined was NOT a backtick.
    // now keep looking until n_backticks are found
    uint32_t cbt = 0; // consecutive backticks
    for (;;) {
      if (lexer->eof(lexer)) {
        return false;
      }
      // found one!
      if (lexer->lookahead == '`') {
        cbt++;
        // are we there yet?
        if (cbt == n_backticks) {
          lexer->advance(lexer, false);
          return true;
        }
      } else { // nope, better reset the count
        cbt = 0;
      }
      // next!
      lexer->advance(lexer, false);
    }

  }

  return false;
}
