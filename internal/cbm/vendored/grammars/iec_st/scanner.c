#include "tree_sitter/parser.h"

#include <string.h>

// ---------------------------------------------------------------------------
// External scanner: reserved block-terminator keywords
//
// IEC 61131-3 Structured Text terminates every block/POU with an `END_*`
// keyword. These are reserved words; they can never be identifiers. Lexing
// them through this scanner (instead of the per-character case-insensitive
// regex helper `kw()` in grammar.js) makes them real reserved tokens that the
// lexer recognizes in *every* state, including during error recovery.
//
// Why this matters: without it, a missing terminator (e.g. a forgotten
// `END_IF`) leaves the parser mid-statement when it meets the next `END_*`.
// Because the regex keyword is not a valid lookahead in that state, the lexer
// falls back to the `identifier` rule and the terminator degrades to a plain
// identifier. tree-sitter then cannot find the enclosing block's terminator
// and collapses the whole POU into one ERROR node. With the terminators
// reserved here, the parser still sees the real `END_FUNCTION_BLOCK` token,
// keeps the `function_block_declaration`, and localizes the error to the
// unterminated inner block.
//
// The token order below MUST stay in lock-step with the `externals` array in
// grammar.js.
// ---------------------------------------------------------------------------

enum TokenType {
  END_IF,
  END_CASE,
  END_FOR,
  END_WHILE,
  END_REPEAT,
  END_VAR,
  END_STRUCT,
  END_TYPE,
  END_PROGRAM,
  END_FUNCTION,
  END_FUNCTION_BLOCK,
  END_INTERFACE,
  END_METHOD,
  END_PROPERTY,
  END_GET,
  END_SET,
  END_NAMESPACE,
  END_CONFIGURATION,
  END_RESOURCE,
};

// Parallel to enum TokenType. Longest spellings come naturally; matching is by
// exact (whole-word) comparison so prefixes such as `END_FUNCTION` vs
// `END_FUNCTION_BLOCK` never collide.
static const char *const KEYWORDS[] = {
  "END_IF",
  "END_CASE",
  "END_FOR",
  "END_WHILE",
  "END_REPEAT",
  "END_VAR",
  "END_STRUCT",
  "END_TYPE",
  "END_PROGRAM",
  "END_FUNCTION",
  "END_FUNCTION_BLOCK",
  "END_INTERFACE",
  "END_METHOD",
  "END_PROPERTY",
  "END_GET",
  "END_SET",
  "END_NAMESPACE",
  "END_CONFIGURATION",
  "END_RESOURCE",
};

#define KEYWORD_COUNT ((int)(sizeof(KEYWORDS) / sizeof(KEYWORDS[0])))

void *tree_sitter_iec61131_3_st_external_scanner_create(void) { return NULL; }

void tree_sitter_iec61131_3_st_external_scanner_destroy(void *payload) {
  (void)payload;
}

unsigned tree_sitter_iec61131_3_st_external_scanner_serialize(void *payload, char *buffer) {
  (void)payload;
  (void)buffer;
  return 0;
}

void tree_sitter_iec61131_3_st_external_scanner_deserialize(void *payload, const char *buffer, unsigned length) {
  (void)payload;
  (void)buffer;
  (void)length;
}

// Identifier rule in grammar.js is ASCII-only: [A-Za-z_][A-Za-z0-9_]*.
// Match it exactly so a word the scanner rejects is re-lexed identically.
static inline bool is_word_start(int32_t c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

static inline bool is_word(int32_t c) {
  return is_word_start(c) || (c >= '0' && c <= '9');
}

static inline char to_upper(int32_t c) {
  return (c >= 'a' && c <= 'z') ? (char)(c - 'a' + 'A') : (char)c;
}

bool tree_sitter_iec61131_3_st_external_scanner_scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  (void)payload;
  (void)valid_symbols;

  // Skip leading whitespace. Comments are handled by tree-sitter's `extras`:
  // if a comment is next, the lookahead is not a word start and we bail out so
  // the normal lexer can consume it.
  while (lexer->lookahead == ' ' || lexer->lookahead == '\t' ||
         lexer->lookahead == '\r' || lexer->lookahead == '\n') {
    lexer->advance(lexer, true);
  }

  if (!is_word_start(lexer->lookahead)) {
    return false;
  }

  // Read the whole word, upper-cased, so the comparison is case-insensitive
  // and prefix collisions are impossible (we compare full words).
  char word[32];
  int len = 0;
  while (is_word(lexer->lookahead)) {
    if (len < (int)sizeof(word) - 1) {
      word[len] = to_upper(lexer->lookahead);
    }
    len++;
    lexer->advance(lexer, false);
  }

  // Any reserved terminator fits in the buffer; an over-long word cannot match.
  if (len >= (int)sizeof(word)) {
    return false;
  }
  word[len] = '\0';

  for (int i = 0; i < KEYWORD_COUNT; i++) {
    if (strcmp(word, KEYWORDS[i]) == 0) {
      lexer->mark_end(lexer);
      lexer->result_symbol = i;
      return true;
    }
  }

  return false;
}
