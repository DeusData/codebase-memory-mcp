#include "tree_sitter/parser.h"

#include <stdlib.h>

#ifdef _MSC_VER
#pragma warning(disable : 4100)
#elif defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

enum TokenType { FAKE_EOL };

/* The scanner's one bit of state -- whether THIS parse has already emitted the
 * end-of-input FAKE_EOL -- lives in the payload, one per parser, which is what
 * tree-sitter's create/destroy contract is for.
 *
 * It used to be a file-scope `static bool`: one object for the entire process,
 * shared by every thread. The grammar needs exactly one FAKE_EOL to close a
 * file, and `!reached_eof` is how the scanner refuses to emit a second one. So
 * when two threads indexed .properties files at the same time, whichever
 * reached EOF first set the flag, and the other thread's `!reached_eof` was
 * false -- it never received the FAKE_EOL its parse was waiting for and sat at
 * end-of-input asking for a token that would never arrive. Measured on
 * 2026-09-19: ~106 million parse operations on a 253-byte file before the
 * per-file budget stopped it, after which the file was dropped from the graph
 * and the index differed from run to run. */
typedef struct {
    bool reached_eof;
} Scanner;

bool tree_sitter_properties_external_scanner_scan(void *payload, TSLexer *lexer,
                                                  const bool *valid_symbols) {
  Scanner *scanner = (Scanner *)payload;
  lexer->result_symbol = FAKE_EOL;
  return scanner->reached_eof =
             !scanner->reached_eof && valid_symbols[FAKE_EOL] && lexer->eof(lexer);
}

/* Wire format unchanged: the state IS the returned length (1 = already emitted,
 * 0 = not), and no bytes are written to the buffer. */
unsigned tree_sitter_properties_external_scanner_serialize(void *payload, char *buffer) {
  return ((Scanner *)payload)->reached_eof;
}

void tree_sitter_properties_external_scanner_deserialize(void *payload, const char *buffer,
                                                         unsigned length) {
  ((Scanner *)payload)->reached_eof = length;
}

void *tree_sitter_properties_external_scanner_create() { return calloc(1, sizeof(Scanner)); }

void tree_sitter_properties_external_scanner_destroy(void *payload) { free(payload); }
