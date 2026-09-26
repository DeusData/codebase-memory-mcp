#include "objectscript_common.h"
#include "tree_sitter/parser.h"
#include <string.h>

// Keep this in sync with grammar externals.
enum TokenType {
  RTN_DOT = OBJECTSCRIPT_CORE_TOKEN_TYPE_MAX,
};

struct ObjectScript_Routine_Scanner {
  struct ObjectScript_Core_Scanner core_scanner;
};

static bool lex_rtn_dot(TSLexer *lexer) {
  lexer->mark_end(lexer);
  if (lexer->get_column(lexer) != 0) return false;
  if (lexer->lookahead != '.') return false;

  advance(lexer);

  // Only match a line that is exactly "." with no trailing content.
  if (lexer->eof(lexer) || lexer->lookahead == '\n' || lexer->lookahead == '\r') {
    lexer->mark_end(lexer);
    lexer->result_symbol = RTN_DOT;
    return true;
  }

  return false;
}

static bool scan(void *payload, TSLexer *lexer, const bool *valid_symbols) {
  struct ObjectScript_Routine_Scanner *scanner =
      (struct ObjectScript_Routine_Scanner *)payload;

  // Tree-sitter marks all terminals as valid during error recovery.
  if (valid_symbols[SENTINEL]) {
    return false;
  }

  if (ObjectScript_Core_Scanner_scan(&scanner->core_scanner, lexer,
                                     valid_symbols)) {
    return true;
  }

  if (scanner->core_scanner.is_rtn_dot) {
    scanner->core_scanner.is_rtn_dot = false;
    lexer->mark_end(lexer);
    lexer->result_symbol = RTN_DOT;
    return true;
  }

  if (valid_symbols[RTN_DOT] && lex_rtn_dot(lexer)) {
    return true;
  }

  return false;
}

void *tree_sitter_objectscript_routine_external_scanner_create() {
  struct ObjectScript_Routine_Scanner *scanner =
      (struct ObjectScript_Routine_Scanner *)calloc(
          1, sizeof(struct ObjectScript_Routine_Scanner));
  ObjectScript_Core_Scanner_init(&scanner->core_scanner);
  scanner->core_scanner.column1_statement_mode = false;
  scanner->core_scanner.routine_token_mode = true;
  return scanner;
}

bool tree_sitter_objectscript_routine_external_scanner_scan(
    void *payload, TSLexer *lexer, const bool *valid_symbols) {
  return scan(payload, lexer, valid_symbols);
}

unsigned tree_sitter_objectscript_routine_external_scanner_serialize(void *payload,
                                                             char *buffer) {
  struct ObjectScript_Routine_Scanner *scanner =
      (struct ObjectScript_Routine_Scanner *)payload;
  memcpy(buffer, scanner, sizeof(struct ObjectScript_Routine_Scanner));
  return sizeof(struct ObjectScript_Routine_Scanner);
}

void tree_sitter_objectscript_routine_external_scanner_deserialize(
    void *payload, const char *buffer, unsigned length) {
  memcpy(payload, buffer, length);
}

void tree_sitter_objectscript_routine_external_scanner_destroy(void *payload) {
  struct ObjectScript_Routine_Scanner *scanner =
      (struct ObjectScript_Routine_Scanner *)payload;
  free(scanner);
}
