#ifndef TREE_SITTER_RST_SCANNER_H
#define TREE_SITTER_RST_SCANNER_H

#define RST_SCANNER_STACK_MAX_CAPACITY 99

#include "../tree_sitter/parser.h"

typedef struct RSTScanner RSTScanner;

/// Wrapper struct around ``TSLexer`` to track indentation levels.
///
/// TODO: track consumed_chars?
struct RSTScanner {
  // Wrappers around the lexer.
  TSLexer* lexer;
  const bool* valid_symbols;

  int32_t lookahead;
  int32_t previous;

  void (*advance)(RSTScanner* scanner);
  void (*skip)(RSTScanner* scanner);

  // Functions for the indentation levels.
  int* indent_stack;
  unsigned length;

  void (*push)(RSTScanner* scanner, int value);
  int (*pop)(RSTScanner* scanner);
  int (*back)(const RSTScanner* scanner);

  // Functions for the scanner.
  unsigned (*serialize)(RSTScanner* scanner, char* buffer);
  void (*deserialize)(RSTScanner* scanner, const char* buffer, unsigned length);
  bool (*scan)(RSTScanner* scanner);
};

static RSTScanner* new_rst_scanner();
static void destroy_rst_scanner(RSTScanner* scanner);

static void rst_scanner_advance(RSTScanner* scanner);
static void rst_scanner_skip(RSTScanner* scanner);

static void rst_scanner_push(RSTScanner* scanner, int value);
static int rst_scanner_pop(RSTScanner* scanner);
static int rst_scanner_back(const RSTScanner* scanner);

static unsigned rst_scanner_serialize(RSTScanner* scanner, char* buffer);
static void rst_scanner_deserialize(RSTScanner* scanner, const char* buffer, unsigned length);
static bool rst_scanner_scan(RSTScanner* scanner);

#endif /* ifndef TREE_SITTER_RST_SCANNER_H */
