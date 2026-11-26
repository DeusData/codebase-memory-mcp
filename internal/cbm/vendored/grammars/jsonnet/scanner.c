// https://github.com/Azganoth/tree-sitter-lua/blob/master/src/scanner.cc
// https://github.com/MunifTanjim/tree-sitter-lua/
// and now here

#include <wctype.h>
#include <stdio.h>
#include "tree_sitter/alloc.h"
#include "tree_sitter/parser.h"

enum TokenType {
  STRING_START,
  STRING_CONTENT,
  STRING_END,
};

static inline void consume(TSLexer *lexer) { lexer->advance(lexer, false); }
static inline void skip(TSLexer *lexer) { lexer->advance(lexer, true); }

static inline bool consume_char(char c, TSLexer *lexer) {
  if (lexer->lookahead != c) {
    return false;
  }

  consume(lexer);
  return true;
}

static inline uint8_t consume_and_count_char(char c, TSLexer *lexer) {
  uint8_t count = 0;
  while (lexer->lookahead == c) {
    ++count;
    consume(lexer);
  }
  return count;
}

static inline void skip_whitespaces(TSLexer *lexer) {
  while (iswspace(lexer->lookahead)) {
    skip(lexer);
  }
}

enum InsideNode { INSIDE_NONE,  INSIDE_STRING };

/* Local patch (see grammars/MANIFEST.md, "Local source patches"): the scanner
 * state lives in the payload, one per parser, which is what tree-sitter's
 * create/destroy contract is for, and deserialize resets it first.
 *
 * Upstream keeps these three fields in file-scope globals: one object for the
 * whole process, shared by every worker thread's parser. tree-sitter restores
 * the state right before each scan and saves it right after, so another
 * thread's write in between changed a scan in flight -- a quoted string ran
 * past its closing quote, and the file came back with ERROR nodes that a
 * sequential parse never produces (go/grafana__loki: 1-2 spurious partial
 * jsonnet files per index, 21-37 when only its 68 jsonnet files are indexed,
 * a different set every run, real definitions and edges lost with them).
 * Upstream's deserialize also ignored the zero-length call that starts each
 * parse, so the state left by the previous parse carried over: after one file
 * with an unterminated quote, no later parse in the process could lex a |||
 * text block. */
typedef struct {
  uint8_t inside_node;
  char ending_char;
  uint8_t level_count;
} Scanner;

void *tree_sitter_jsonnet_external_scanner_create() { return ts_calloc(1, sizeof(Scanner)); }
void tree_sitter_jsonnet_external_scanner_destroy(void *payload) { ts_free(payload); }

static inline void reset_state(Scanner *s) {
  s->inside_node = INSIDE_NONE;
  s->ending_char = 0;
  s->level_count = 0;
}

unsigned tree_sitter_jsonnet_external_scanner_serialize(void *payload, char *buffer) {
  Scanner *s = (Scanner *)payload;
  buffer[0] = s->inside_node;
  buffer[1] = s->ending_char;
  buffer[2] = s->level_count;
  return 3;
}

void tree_sitter_jsonnet_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
  Scanner *s = (Scanner *)payload;
  reset_state(s); /* length 0 = start of a parse: no state carried over */
  if (length == 0) return;
  s->inside_node = buffer[0];
  if (length == 1) return;
  s->ending_char = buffer[1];
  if (length == 2) return;
  s->level_count = buffer[2];
}

static bool scan_block_start(TSLexer *lexer) {
  if (consume_char('|', lexer) && consume_char('|', lexer) && consume_char('|', lexer)) {
    return true;
  }

  return false;
}

static bool scan_block_end(TSLexer *lexer) {
  if (consume_char('|', lexer) && consume_char('|', lexer) && consume_char('|', lexer)) {
    return true;
  }

  return false;
}

static bool scan_block_content(TSLexer *lexer) {
  while (lexer->lookahead != 0) {
    if (lexer->lookahead == '|') {
      lexer->mark_end(lexer);

      if (scan_block_end(lexer)) {
        return true;
      }
    } else {
      consume(lexer);
    }
  }

  return false;
}

static bool scan_string_start(Scanner *s, TSLexer *lexer) {
  if (lexer->lookahead == '"' || lexer->lookahead == '\'') {
    s->inside_node = INSIDE_STRING;
    s->ending_char = lexer->lookahead;
    consume(lexer);
    return true;
  }

  if (scan_block_start(lexer)) {
    s->inside_node = INSIDE_STRING;
    return true;
  }

  return false;
}

static bool scan_string_end(Scanner *s, TSLexer *lexer) {
  if (s->ending_char == 0) { // block string
    return scan_block_end(lexer);
  }

  if (consume_char(s->ending_char, lexer)) {
    return true;
  }

  return false;
}

static bool scan_string_content(Scanner *s, TSLexer *lexer) {
  if (s->ending_char == 0) { // block string
    return scan_block_content(lexer);
  }

  while (lexer->lookahead != '\n' && lexer->lookahead != 0 && lexer->lookahead != s->ending_char) {
    while (consume_char('\\', lexer) && consume_char('z', lexer)) continue;

    if (lexer->lookahead == 0) {
      return true;
    }

    consume(lexer);
  }

  return true;
}

bool tree_sitter_jsonnet_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  Scanner *s = (Scanner *)payload;
  if (s->inside_node == INSIDE_STRING) {
    if (valid_symbols[STRING_END] && scan_string_end(s, lexer)) {
      reset_state(s);
      lexer->result_symbol = STRING_END;
      return true;
    }

    if (valid_symbols[STRING_CONTENT] && scan_string_content(s, lexer)) {
      lexer->result_symbol = STRING_CONTENT;
      return true;
    }

    return false;
  }

  skip_whitespaces(lexer);

  if (valid_symbols[STRING_START] && scan_string_start(s, lexer)) {
    lexer->result_symbol = STRING_START;
    return true;
  }


  return false;
}